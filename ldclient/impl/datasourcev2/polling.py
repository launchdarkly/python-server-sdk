"""
This module contains the implementations of a polling synchronizer and
initializer, along with any required supporting classes and protocols.
"""

import json
from abc import abstractmethod
from collections import namedtuple
from threading import Event
from time import time
from typing import Generator, Mapping, Optional, Protocol, Tuple
from urllib import parse

import urllib3

from ldclient.config import Config, DataSourceBuilder, HTTPConfig
from ldclient.impl.datasource.feature_requester import FDV1_POLLING_ENDPOINT
from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    EventName,
    PutObject
)
from ldclient.impl.http import HTTPFactory, _base_headers
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
    Basis,
    BasisResult,
    ChangeSet,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    Initializer,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    ServerIntent,
    Synchronizer,
    Update
)

FDV2_POLLING_ENDPOINT = "/sdk/poll"


PollingResult = _Result[Tuple[ChangeSet, Mapping], str]


class Requester(Protocol):  # pylint: disable=too-few-public-methods
    """
    Requester allows PollingDataSource to delegate fetching data to
    another component.

    This is useful for testing the PollingDataSource without needing to set up
    a test HTTP server.
    """

    @abstractmethod
    def fetch(self, selector: Optional[Selector]) -> PollingResult:
        """
        Fetches the data for the given selector.
        Returns a Result containing a tuple of ChangeSet and any request headers,
        or an error if the data could not be retrieved.
        """
        raise NotImplementedError


CacheEntry = namedtuple("CacheEntry", ["data", "etag"])


class PollingDataSource(Initializer, Synchronizer):
    """
    PollingDataSource is a data source that can retrieve information from
    LaunchDarkly either as an Initializer or as a Synchronizer.
    """

    def __init__(
        self,
        poll_interval: float,
        requester: Requester,
    ):
        self._requester = requester
        self._poll_interval = poll_interval
        self._interrupt_event = Event()
        self._stop = Event()

    @property
    def name(self) -> str:
        """Returns the name of the initializer."""
        return "PollingDataSourceV2"

    def fetch(self, ss: SelectorStore) -> BasisResult:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        return self._poll(ss)

    def sync(self, ss: SelectorStore) -> Generator[Update, None, None]:
        """
        sync begins the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        log.info("Starting PollingDataSourceV2 synchronizer")
        self._interrupt_event.clear()
        self._stop.clear()
        while self._stop.is_set() is False:
            result = self._requester.fetch(ss.selector())
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
                            revert_to_fdv1=True,
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
                        self._interrupt_event.wait(self._poll_interval)
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
                    revert_to_fdv1=headers.get(_LD_FD_FALLBACK_HEADER) == 'true'
                )

            if self._interrupt_event.wait(self._poll_interval):
                break

    def stop(self):
        """Stops the synchronizer."""
        log.info("Stopping PollingDataSourceV2 synchronizer")
        self._interrupt_event.set()
        self._stop.set()

    def _poll(self, ss: SelectorStore) -> BasisResult:
        try:
            result = self._requester.fetch(ss.selector())

            if isinstance(result, _Fail):
                if isinstance(result.exception, UnsuccessfulResponseException):
                    status_code = result.exception.status
                    http_error_message_result = http_error_message(
                        status_code, "polling request"
                    )
                    if is_http_error_recoverable(status_code):
                        log.warning(http_error_message_result)

                    return _Fail(
                        error=http_error_message_result, exception=result.exception
                    )

                return _Fail(
                    error=result.error or "Failed to request payload",
                    exception=result.exception,
                )

            (change_set, headers) = result.value

            env_id = headers.get(_LD_ENVID_HEADER)
            if not isinstance(env_id, str):
                env_id = None

            basis = Basis(
                change_set=change_set,
                persist=change_set.selector.is_defined(),
                environment_id=env_id,
            )

            return _Success(value=basis)
        except Exception as e:  # pylint: disable=broad-except
            msg = f"Error: Exception encountered when updating flags. {e}"
            log.exception(msg)

            return _Fail(error=msg, exception=e)


# pylint: disable=too-few-public-methods
class Urllib3PollingRequester(Requester):
    """
    Urllib3PollingRequester is a Requester that uses urllib3 to make HTTP
    requests.
    """

    def __init__(self, config: Config, base_uri: str, http_options: HTTPConfig):
        self._etag = None
        factory = HTTPFactory(_base_headers(config), http_options)
        self._http = factory.create_pool_manager(1, base_uri)
        self._http_options = http_options
        self._config = config
        self._poll_uri = base_uri + FDV2_POLLING_ENDPOINT

    def fetch(self, selector: Optional[Selector]) -> PollingResult:
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

        response = self._http.request(
            "GET",
            uri,
            headers=hdrs,
            timeout=urllib3.Timeout(
                connect=self._http_options.connect_timeout,
                read=self._http_options.read_timeout,
            ),
            retries=1,
        )
        headers = response.headers

        if response.status >= 400:
            return _Fail(
                f"HTTP error {response}", UnsuccessfulResponseException(response.status),
                headers=headers,
            )

        if response.status == 304:
            return _Success(value=(ChangeSetBuilder.no_changes(), headers))

        data = json.loads(response.data.decode("UTF-8"))
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


# pylint: disable=too-many-branches,too-many-return-statements
def polling_payload_to_changeset(data: dict) -> _Result[ChangeSet, str]:
    """
    Converts a polling payload into a ChangeSet.
    """
    if "events" not in data or not isinstance(data["events"], list):
        return _Fail(error="Invalid payload: 'events' key is missing or not a list")

    builder = ChangeSetBuilder()

    for event in data["events"]:
        if not isinstance(event, dict):
            return _Fail(error="Invalid payload: 'events' must be a list of objects")

        if "event" not in event:
            continue

        if event["event"] == EventName.SERVER_INTENT:
            try:
                server_intent = ServerIntent.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in server intent", exception=err)

            if server_intent.payload.code == IntentCode.TRANSFER_NONE:
                return _Success(ChangeSetBuilder.no_changes())

            builder.start(server_intent.payload.code)
        elif event["event"] == EventName.PUT_OBJECT:
            try:
                put = PutObject.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in put object", exception=err)

            builder.add_put(put.kind, put.key, put.version, put.object)
        elif event["event"] == EventName.DELETE_OBJECT:
            try:
                delete_object = DeleteObject.from_dict(event["data"])
            except ValueError as err:
                return _Fail(error="Invalid JSON in delete object", exception=err)

            builder.add_delete(
                delete_object.kind, delete_object.key, delete_object.version
            )
        elif event["event"] == EventName.PAYLOAD_TRANSFERRED:
            try:
                selector = Selector.from_dict(event["data"])
                changeset = builder.finish(selector)

                return _Success(value=changeset)
            except ValueError as err:
                return _Fail(
                    error="Invalid JSON in payload transferred object", exception=err
                )

    return _Fail(error="didn't receive any known protocol events in polling payload")


class PollingDataSourceBuilder(DataSourceBuilder):
    """
    Builder for a PollingDataSource.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__poll_interval: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None
        self.__requester: Optional[Requester] = None

    def base_uri(self, uri: str) -> 'PollingDataSourceBuilder':
        """Sets the base URI for the streaming data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def poll_interval(self, poll_interval: float) -> 'PollingDataSourceBuilder':
        """Sets the polling interval for the PollingDataSource."""
        self.__poll_interval = poll_interval
        return self

    def http_options(self, http_options: HTTPConfig) -> 'PollingDataSourceBuilder':
        """Sets the HTTP options for the streaming data source."""
        self.__http_options = http_options
        return self

    def requester(self, requester: Requester) -> 'PollingDataSourceBuilder':
        """Sets a custom Requester for the PollingDataSource."""
        self.__requester = requester
        return self

    def build(self, config: Config) -> PollingDataSource:
        """Builds the PollingDataSource with the configured parameters."""
        requester = (
            self.__requester
            if self.__requester is not None
            else Urllib3PollingRequester(
                config,
                self.__base_uri or config.base_uri,
                self.__http_options or config.http
            )
        )

        return PollingDataSource(
            poll_interval=self.__poll_interval or config.poll_interval,
            requester=requester
        )


class FallbackToFDv1PollingDataSourceBuilder(DataSourceBuilder):
    """
    Builder for a PollingDataSource that falls back to Flag Delivery v1.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__poll_interval: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None

    def base_uri(self, uri: str) -> 'FallbackToFDv1PollingDataSourceBuilder':
        """Sets the base URI for the data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def poll_interval(self, poll_interval: float) -> 'FallbackToFDv1PollingDataSourceBuilder':
        """Sets the polling interval for the data source."""
        self.__poll_interval = poll_interval
        return self

    def http_options(self, http_options: HTTPConfig) -> 'FallbackToFDv1PollingDataSourceBuilder':
        """Sets the HTTP options for the data source."""
        self.__http_options = http_options
        return self

    def build(self, config: Config) -> PollingDataSource:
        """Builds the PollingDataSource with the configured parameters."""
        builder = PollingDataSourceBuilder()
        builder.requester(
            Urllib3FDv1PollingRequester(
                config,
                self.__base_uri or config.base_uri,
                self.__http_options or config.http
            )
        )
        builder.poll_interval(self.__poll_interval or config.poll_interval)

        return builder.build(config)


# pylint: disable=too-few-public-methods
class Urllib3FDv1PollingRequester(Requester):
    """
    Urllib3PollingRequesterFDv1 is a Requester that uses urllib3 to make HTTP
    requests.
    """

    def __init__(self, config: Config, base_uri: str, http_options: HTTPConfig):
        self._etag = None
        self._http = HTTPFactory(_base_headers(config), http_options).create_pool_manager(
            1, base_uri
        )
        self._http_options = http_options
        self._config = config
        self._poll_uri = base_uri + FDV1_POLLING_ENDPOINT

    def fetch(self, selector: Optional[Selector]) -> PollingResult:
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

        response = self._http.request(
            "GET",
            uri,
            headers=hdrs,
            timeout=urllib3.Timeout(
                connect=self._http_options.connect_timeout,
                read=self._http_options.read_timeout,
            ),
            retries=1,
        )

        headers = response.headers
        if response.status >= 400:
            return _Fail(
                f"HTTP error {response}", UnsuccessfulResponseException(response.status),
                headers=headers
            )

        if response.status == 304:
            return _Success(value=(ChangeSetBuilder.no_changes(), headers))

        data = json.loads(response.data.decode("UTF-8"))
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


# pylint: disable=too-many-branches,too-many-return-statements
def fdv1_polling_payload_to_changeset(data: dict) -> _Result[ChangeSet, str]:
    """
    Converts a fdv1 polling payload into a ChangeSet.
    """
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    selector = Selector.no_selector()

    # FDv1 uses "flags" instead of "features", so we need to map accordingly
    # Map FDv1 JSON keys to ObjectKind enum values
    kind_mappings = [
        (ObjectKind.FLAG, "flags"),
        (ObjectKind.SEGMENT, "segments")
    ]

    for kind, fdv1_key in kind_mappings:
        kind_data = data.get(fdv1_key)
        if kind_data is None:
            continue
        if not isinstance(kind_data, dict):
            return _Fail(error=f"Invalid format: {fdv1_key} is not a dictionary")

        for key in kind_data:
            flag_or_segment = kind_data.get(key)
            if flag_or_segment is None or not isinstance(flag_or_segment, dict):
                return _Fail(error=f"Invalid format: {key} is not a dictionary")

            version = flag_or_segment.get('version')
            if version is None:
                return _Fail(error=f"Invalid format: {key} does not have a version set")

            builder.add_put(kind, key, version, flag_or_segment)

    return _Success(builder.finish(selector))
