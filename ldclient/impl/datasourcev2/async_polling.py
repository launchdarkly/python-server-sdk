"""
This module contains the implementations of a polling synchronizer and
initializer, along with any required supporting classes and protocols.
"""

import json
from abc import abstractmethod
from collections import namedtuple
from typing import AsyncGenerator, Mapping, Optional, Protocol, Tuple
from urllib import parse

from ldclient.config import (
    DataSourceBuilder,
    DataSourceBuilderConfig,
    HTTPConfig
)
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.aio.transport import AsyncHTTPTransport
from ldclient.impl.datasource.async_feature_requester import (
    FDV1_POLLING_ENDPOINT
)
from ldclient.impl.datasourcev2.polling_common import (
    PollAction,
    fdv1_polling_payload_to_changeset,
    map_polling_result,
    polling_payload_to_changeset,
    polling_result_to_basis
)
from ldclient.impl.util import (
    UnsuccessfulResponseException,
    _Fail,
    _headers,
    _Result,
    _Success,
    log
)
from ldclient.interfaces import (
    AsyncInitializer,
    AsyncSynchronizer,
    BasisResult,
    ChangeSet,
    ChangeSetBuilder,
    Selector,
    SelectorStore,
    Update
)

FDV2_POLLING_ENDPOINT = "/sdk/poll"


PollingResult = _Result[Tuple[ChangeSet, Mapping], str]


class AsyncRequester(Protocol):  # pylint: disable=too-few-public-methods
    """
    AsyncRequester allows AsyncPollingDataSource to delegate fetching data to
    another component.

    This is useful for testing the AsyncPollingDataSource without needing to set up
    a test HTTP server.
    """

    @abstractmethod
    async def fetch(self, selector: Optional[Selector]) -> PollingResult:
        """
        Fetches the data for the given selector.
        Returns a Result containing a tuple of ChangeSet and any request headers,
        or an error if the data could not be retrieved.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """
        Releases any resources (such as an HTTP transport) owned by the
        requester.
        """
        raise NotImplementedError


CacheEntry = namedtuple("CacheEntry", ["data", "etag"])


class AsyncPollingDataSource(AsyncInitializer, AsyncSynchronizer):
    """
    AsyncPollingDataSource is a data source that can retrieve information from
    LaunchDarkly either as an initializer or as a synchronizer.
    """

    def __init__(
        self,
        poll_interval: float,
        requester: AsyncRequester,
    ):
        self._requester = requester
        self._poll_interval = poll_interval
        self._interrupt_event = AsyncEvent()
        self._stop = AsyncEvent()

    @property
    def name(self) -> str:
        """Returns the name of the initializer."""
        return "PollingDataSourceV2"

    async def fetch(self, ss: SelectorStore) -> BasisResult:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        return await self._poll(ss)

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        """
        sync begins the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        log.info("Starting PollingDataSourceV2 synchronizer")
        self._interrupt_event.clear()
        self._stop.clear()
        try:
            while self._stop.is_set() is False:
                result = await self._requester.fetch(ss.selector())
                decision = map_polling_result(result)
                yield decision.update

                if decision.control is PollAction.BREAK:
                    break
                if decision.control is PollAction.WAIT_CONTINUE:
                    await self._interrupt_event.wait(self._poll_interval)
                    continue
                if await self._interrupt_event.wait(self._poll_interval):
                    break
        finally:
            await self._requester.close()

    async def stop(self):
        """Signals the synchronizer to stop."""
        log.info("Stopping PollingDataSourceV2 synchronizer")
        self._interrupt_event.set()
        self._stop.set()

    async def _poll(self, ss: SelectorStore) -> BasisResult:
        try:
            result = await self._requester.fetch(ss.selector())
            return polling_result_to_basis(result)
        except Exception as e:  # pylint: disable=broad-except
            msg = f"Error: Exception encountered when updating flags. {e}"
            log.exception(msg)

            return _Fail(error=msg, exception=e)


# pylint: disable=too-few-public-methods
class AiohttpPollingRequester(AsyncRequester):
    """
    A requester implementation that issues HTTP requests through the SDK's
    HTTP transport.
    """

    def __init__(
        self,
        config: DataSourceBuilderConfig,
        base_uri: str,
        http_options: HTTPConfig,
        session=None,
    ):
        self._etag: Optional[str] = None
        self._http = AsyncHTTPTransport(
            config,
            client=session,
            http_options=http_options,
        )
        self._http_options = http_options
        self._config = config
        self._poll_uri = base_uri + FDV2_POLLING_ENDPOINT

    async def fetch(self, selector: Optional[Selector]) -> PollingResult:
        """
        Fetches the data for the given selector.
        Returns a Result containing a tuple of ChangeSet and any request headers,
        or an error if the data could not be retrieved.
        """
        query_params = {}
        if self._config.payload_filter_key is not None:
            query_params["filter"] = self._config.payload_filter_key

        if selector is not None and selector.is_defined():
            query_params["basis"] = selector.state

        uri = self._poll_uri
        if len(query_params) > 0:
            filter_query = parse.urlencode(query_params)
            uri += f"?{filter_query}"

        hdrs = _headers(self._config)
        hdrs["Accept-Encoding"] = "gzip"

        if self._etag is not None:
            hdrs["If-None-Match"] = self._etag

        response = await self._http.request(
            "GET",
            uri,
            headers=hdrs,
        )
        headers = response.headers

        if response.status >= 400:
            return _Fail(
                f"HTTP error {response.status}", UnsuccessfulResponseException(response.status),
                headers=headers,
            )

        if response.status == 304:
            return _Success(value=(ChangeSetBuilder.no_changes(), headers))

        data = json.loads(response.body)
        etag = headers.get("ETag")

        if etag is not None:
            self._etag = etag

        log.debug(
            "%s response status:[%d] ETag:[%s]",
            uri,
            response.status,
            etag,
        )

        changeset_result = polling_payload_to_changeset(data)
        if isinstance(changeset_result, _Success):
            return _Success(value=(changeset_result.value, headers))

        return _Fail(
            error=changeset_result.error,
            exception=changeset_result.exception,
            headers=headers,  # type: ignore
        )

    async def close(self) -> None:
        """Closes the requester's HTTP transport."""
        await self._http.close()


class AsyncPollingDataSourceBuilder(DataSourceBuilder[AsyncPollingDataSource]):
    """
    Builder for a AsyncPollingDataSource.

    The built polling data source implements both :class:`AsyncInitializer` and
    :class:`AsyncSynchronizer`, so this builder can be used in either role.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__poll_interval: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None
        self.__requester: Optional[AsyncRequester] = None
        self.__session = None

    def base_uri(self, uri: str) -> 'AsyncPollingDataSourceBuilder':
        """Sets the base URI for the streaming data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def poll_interval(self, poll_interval: float) -> 'AsyncPollingDataSourceBuilder':
        """Sets the polling interval for the AsyncPollingDataSource."""
        self.__poll_interval = poll_interval
        return self

    def http_options(self, http_options: HTTPConfig) -> 'AsyncPollingDataSourceBuilder':
        """Sets the HTTP options for the streaming data source."""
        self.__http_options = http_options
        return self

    def requester(self, requester: AsyncRequester) -> 'AsyncPollingDataSourceBuilder':
        """Sets a custom AsyncRequester for the AsyncPollingDataSource."""
        self.__requester = requester
        return self

    def session(self, session) -> 'AsyncPollingDataSourceBuilder':
        """Sets the aiohttp session used for HTTP requests."""
        self.__session = session
        return self

    def build(self, config: DataSourceBuilderConfig) -> AsyncPollingDataSource:
        """Builds the AsyncPollingDataSource with the configured parameters."""
        requester = (
            self.__requester
            if self.__requester is not None
            else AiohttpPollingRequester(
                config,
                self.__base_uri or config.base_uri,
                self.__http_options or config.http,
                session=self.__session,
            )
        )

        return AsyncPollingDataSource(
            poll_interval=self.__poll_interval or config.poll_interval,
            requester=requester
        )


class AsyncFallbackToFDv1PollingDataSourceBuilder(DataSourceBuilder[AsyncPollingDataSource]):
    """
    Builder for a AsyncPollingDataSource that falls back to Flag Delivery v1.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__poll_interval: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None
        self.__session = None

    def base_uri(self, uri: str) -> 'AsyncFallbackToFDv1PollingDataSourceBuilder':
        """Sets the base URI for the data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def poll_interval(self, poll_interval: float) -> 'AsyncFallbackToFDv1PollingDataSourceBuilder':
        """Sets the polling interval for the data source."""
        self.__poll_interval = poll_interval
        return self

    def http_options(self, http_options: HTTPConfig) -> 'AsyncFallbackToFDv1PollingDataSourceBuilder':
        """Sets the HTTP options for the data source."""
        self.__http_options = http_options
        return self

    def session(self, session) -> 'AsyncFallbackToFDv1PollingDataSourceBuilder':
        """Sets the aiohttp session used for HTTP requests."""
        self.__session = session
        return self

    def build(self, config: DataSourceBuilderConfig) -> AsyncPollingDataSource:
        """Builds the AsyncPollingDataSource with the configured parameters."""
        builder = AsyncPollingDataSourceBuilder()
        builder.requester(
            AiohttpFDv1PollingRequester(
                config,
                self.__base_uri or config.base_uri,
                self.__http_options or config.http,
                session=self.__session,
            )
        )
        builder.poll_interval(self.__poll_interval or config.poll_interval)

        return builder.build(config)


# pylint: disable=too-few-public-methods
class AiohttpFDv1PollingRequester(AsyncRequester):
    """
    A requester implementation for the Flag Delivery v1 polling endpoint that
    issues HTTP requests through the SDK's HTTP transport.
    """

    def __init__(
        self,
        config: DataSourceBuilderConfig,
        base_uri: str,
        http_options: HTTPConfig,
        session=None,
    ):
        self._etag: Optional[str] = None
        self._http = AsyncHTTPTransport(
            config,
            client=session,
            http_options=http_options,
        )
        self._http_options = http_options
        self._config = config
        self._poll_uri = base_uri + FDV1_POLLING_ENDPOINT

    async def fetch(self, selector: Optional[Selector]) -> PollingResult:
        """
        Fetches the data for the given selector.
        Returns a Result containing a tuple of ChangeSet and any request headers,
        or an error if the data could not be retrieved.
        """
        query_params = {}
        if self._config.payload_filter_key is not None:
            query_params["filter"] = self._config.payload_filter_key

        uri = self._poll_uri
        if len(query_params) > 0:
            filter_query = parse.urlencode(query_params)
            uri += f"?{filter_query}"

        hdrs = _headers(self._config)
        hdrs["Accept-Encoding"] = "gzip"

        if self._etag is not None:
            hdrs["If-None-Match"] = self._etag

        response = await self._http.request(
            "GET",
            uri,
            headers=hdrs,
        )

        headers = response.headers
        if response.status >= 400:
            return _Fail(
                f"HTTP error {response.status}", UnsuccessfulResponseException(response.status),
                headers=headers
            )

        if response.status == 304:
            return _Success(value=(ChangeSetBuilder.no_changes(), headers))

        data = json.loads(response.body)
        etag = headers.get("ETag")

        if etag is not None:
            self._etag = etag

        log.debug(
            "%s response status:[%d] ETag:[%s]",
            uri,
            response.status,
            etag,
        )

        changeset_result = fdv1_polling_payload_to_changeset(data)
        if isinstance(changeset_result, _Success):
            return _Success(value=(changeset_result.value, headers))

        return _Fail(
            error=changeset_result.error,
            exception=changeset_result.exception,
            headers=headers,
        )

    async def close(self) -> None:
        """Closes the requester's HTTP transport."""
        await self._http.close()
