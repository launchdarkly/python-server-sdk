"""
This module contains the implementations of a polling synchronizer and
initializer, along with any required supporting classes and protocols.
"""

import json
from abc import abstractmethod
from collections import namedtuple
from time import time
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
    fdv1_polling_payload_to_changeset,
    polling_payload_to_changeset
)
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    UnsuccessfulResponseException,
    _Fail,
    _headers,
    _Result,
    _Success,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    AsyncInitializer,
    AsyncSynchronizer,
    Basis,
    BasisResult,
    ChangeSet,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
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
        while self._stop.is_set() is False:
            result = await self._requester.fetch(ss.selector())
            if isinstance(result, _Fail):
                fallback = None
                envid = None

                if result.headers is not None:
                    fallback = result.headers.get(_LD_FD_FALLBACK_HEADER) == 'true'
                    envid = result.headers.get(_LD_ENVID_HEADER)

                if isinstance(result.exception, UnsuccessfulResponseException):
                    error_info = DataSourceErrorInfo(
                        kind=DataSourceErrorKind.ERROR_RESPONSE,
                        status_code=result.exception.status,
                        time=time(),
                        message=http_error_message(
                            result.exception.status, "polling request"
                        ),
                    )

                    if fallback:
                        yield Update(
                            state=DataSourceState.OFF,
                            error=error_info,
                            fallback_to_fdv1=True,
                            environment_id=envid,
                        )
                        break

                    status_code = result.exception.status
                    if is_http_error_recoverable(status_code):
                        yield Update(
                            state=DataSourceState.INTERRUPTED,
                            error=error_info,
                            environment_id=envid,
                        )
                        await self._interrupt_event.wait(self._poll_interval)
                        continue

                    yield Update(
                        state=DataSourceState.OFF,
                        error=error_info,
                        environment_id=envid,
                    )
                    break

                error_info = DataSourceErrorInfo(
                    kind=DataSourceErrorKind.NETWORK_ERROR,
                    time=time(),
                    status_code=0,
                    message=result.error,
                )

                # Even a non-HTTP error (e.g. malformed JSON) can carry the fallback
                # header. If so, halt rather than retrying the FDv2 endpoint.
                if fallback:
                    yield Update(
                        state=DataSourceState.OFF,
                        error=error_info,
                        fallback_to_fdv1=True,
                        environment_id=envid,
                    )
                    break

                yield Update(
                    state=DataSourceState.INTERRUPTED,
                    error=error_info,
                    environment_id=envid,
                )
            else:
                (change_set, headers) = result.value
                yield Update(
                    state=DataSourceState.VALID,
                    change_set=change_set,
                    environment_id=headers.get(_LD_ENVID_HEADER),
                    fallback_to_fdv1=headers.get(_LD_FD_FALLBACK_HEADER) == 'true'
                )

            if await self._interrupt_event.wait(self._poll_interval):
                break

    async def stop(self):
        """Stops the synchronizer."""
        log.info("Stopping PollingDataSourceV2 synchronizer")
        self._interrupt_event.set()
        self._stop.set()

    async def _poll(self, ss: SelectorStore) -> BasisResult:
        try:
            result = await self._requester.fetch(ss.selector())

            if isinstance(result, _Fail):
                if isinstance(result.exception, UnsuccessfulResponseException):
                    status_code = result.exception.status
                    http_error_message_result = http_error_message(
                        status_code, "polling request"
                    )
                    if is_http_error_recoverable(status_code):
                        log.warning(http_error_message_result)

                    # Forward any response headers so callers (e.g. FDv2 datasystem)
                    # can read the X-LD-FD-Fallback directive even on error.
                    return _Fail(
                        error=http_error_message_result,
                        exception=result.exception,
                        headers=result.headers,
                    )

                return _Fail(
                    error=result.error or "Failed to request payload",
                    exception=result.exception,
                    headers=result.headers,
                )

            (change_set, headers) = result.value

            env_id = headers.get(_LD_ENVID_HEADER)
            if not isinstance(env_id, str):
                env_id = None

            basis = Basis(
                change_set=change_set,
                persist=change_set.selector.is_defined(),
                environment_id=env_id,
                fallback_to_fdv1=headers.get(_LD_FD_FALLBACK_HEADER) == 'true',
            )

            return _Success(value=basis)
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
            query_params["selector"] = selector.state

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


class AsyncPollingDataSourceBuilder(DataSourceBuilder):
    """
    Builder for a AsyncPollingDataSource.
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


class AsyncFallbackToFDv1PollingDataSourceBuilder(DataSourceBuilder):
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
