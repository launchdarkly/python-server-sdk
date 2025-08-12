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

from ldclient.config import Config
from ldclient.impl.datasystem import BasisResult, Update
from ldclient.impl.datasystem.protocolv2 import (
    Basis,
    ChangeSet,
    ChangeSetBuilder,
    DeleteObject,
    EventName,
    IntentCode,
    PutObject,
    Selector,
    ServerIntent
)
from ldclient.impl.http import _http_factory
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import (
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
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState
)

POLLING_ENDPOINT = "/sdk/poll"


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


class PollingDataSource:
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
        self._event = Event()
        self._task = RepeatingTask(
            "ldclient.datasource.polling", poll_interval, 0, self._poll
        )

    def name(self) -> str:
        """Returns the name of the initializer."""
        return "PollingDataSourceV2"

    def fetch(self) -> BasisResult:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        return self._poll()

    def sync(self) -> Generator[Update, None, None]:
        """
        sync begins the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        log.info("Starting PollingDataSourceV2 synchronizer")
        while True:
            result = self._requester.fetch(None)
            if isinstance(result, _Fail):
                if isinstance(result.exception, UnsuccessfulResponseException):
                    error_info = DataSourceErrorInfo(
                        kind=DataSourceErrorKind.ERROR_RESPONSE,
                        status_code=result.exception.status,
                        time=time(),
                        message=http_error_message(
                            result.exception.status, "polling request"
                        ),
                    )

                    status_code = result.exception.status
                    if is_http_error_recoverable(status_code):
                        # TODO(fdv2): Add support for environment ID
                        yield Update(
                            state=DataSourceState.INTERRUPTED,
                            error=error_info,
                        )
                        continue

                    # TODO(fdv2): Add support for environment ID
                    yield Update(
                        state=DataSourceState.OFF,
                        error=error_info,
                    )
                    break

                error_info = DataSourceErrorInfo(
                    kind=DataSourceErrorKind.NETWORK_ERROR,
                    time=time(),
                    status_code=0,
                    message=result.error,
                )

                # TODO(fdv2): Go has a designation here to handle JSON decoding separately.
                # TODO(fdv2): Add support for environment ID
                yield Update(
                    state=DataSourceState.INTERRUPTED,
                    error=error_info,
                )
            else:
                (change_set, headers) = result.value
                yield Update(
                    state=DataSourceState.VALID,
                    change_set=change_set,
                    environment_id=headers.get("X-LD-EnvID"),
                )

            if self._event.wait(self._poll_interval):
                break

    def _poll(self) -> BasisResult:
        try:
            # TODO(fdv2): Need to pass the selector through
            result = self._requester.fetch(None)

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

            env_id = headers.get("X-LD-EnvID")
            if not isinstance(env_id, str):
                env_id = None

            basis = Basis(
                change_set=change_set,
                persist=change_set.selector is not None,
                environment_id=env_id,
            )

            return _Success(value=basis)
        except Exception as e:  # pylint: disable=broad-except
            msg = f"Error: Exception encountered when updating flags. {e}"
            log.exception(msg)

            return _Fail(error=msg, exception=e)


# pylint: disable=too-few-public-methods
class Urllib3PollingRequester:
    """
    Urllib3PollingRequester is a Requester that uses urllib3 to make HTTP
    requests.
    """

    def __init__(self, config: Config):
        self._etag = None
        self._http = _http_factory(config).create_pool_manager(1, config.base_uri)
        self._config = config
        self._poll_uri = config.base_uri + POLLING_ENDPOINT

    def fetch(self, selector: Optional[Selector]) -> PollingResult:
        """
        Fetches the data for the given selector.
        Returns a Result containing a tuple of ChangeSet and any request headers,
        or an error if the data could not be retrieved.
        """
        query_params = {}
        if self._config.payload_filter_key is not None:
            query_params["filter"] = self._config.payload_filter_key

        if selector is not None:
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
                connect=self._config.http.connect_timeout,
                read=self._config.http.read_timeout,
            ),
            retries=1,
        )

        if response.status >= 400:
            return _Fail(
                f"HTTP error {response}", UnsuccessfulResponseException(response.status)
            )

        headers = response.headers

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


class PollingDataSourceBuilder:
    """
    Builder for a PollingDataSource.
    """

    def __init__(self, config: Config):
        self._config = config
        self._requester: Optional[Requester] = None

    def requester(self, requester: Requester) -> "PollingDataSourceBuilder":
        """Sets a custom Requester for the PollingDataSource."""
        self._requester = requester
        return self

    def build(self) -> PollingDataSource:
        """Builds the PollingDataSource with the configured parameters."""
        requester = (
            self._requester
            if self._requester is not None
            else Urllib3PollingRequester(self._config)
        )

        return PollingDataSource(
            poll_interval=self._config.poll_interval, requester=requester
        )
