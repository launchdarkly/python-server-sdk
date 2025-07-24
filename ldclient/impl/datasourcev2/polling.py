"""
Default implementation of the polling synchronizer and initializer.
"""

import json
from abc import abstractmethod
from collections import namedtuple
from collections.abc import Mapping
from typing import Optional, Protocol, Tuple
from urllib import parse

import urllib3

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
    Result,
    UnsuccessfulResponseException,
    _Fail,
    _headers,
    _Result,
    _Success,
    http_error_message,
    is_http_error_recoverable,
    log
)

POLLING_ENDPOINT = "/sdk/poll"

PollingResult = _Result[Tuple[ChangeSet, Mapping], str]


class PollingRequester(Protocol):  # pylint: disable=too-few-public-methods
    """
    PollingRequester allows PollingDataSource to delegate fetching data to
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
        requester: PollingRequester,
    ):
        self._requester = requester
        self._task = RepeatingTask(
            "ldclient.datasource.polling", poll_interval, 0, self._poll
        )

    def name(self) -> str:
        """Returns the name of the initializer."""
        return "PollingDataSourceV2"

    def fetch(self) -> Result:  # Result[Basis]:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        return self._poll()

    # TODO(fdv2): This will need to be converted into a synchronizer at some point.
    # def start(self):
    #     log.info(
    #         "Starting PollingUpdateProcessor with request interval: "
    #         + str(self._config.poll_interval)
    #     )
    #     self._task.start()

    def _poll(self) -> Result:  # Result[Basis]:
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

                    return Result.fail(http_error_message_result, result.exception)

                return Result.fail(
                    result.error or "Failed to request payload", result.exception
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

            return Result.success(basis)
        except Exception as e:
            msg = f"Error: Exception encountered when updating flags. {e}"
            log.exception(msg)

            return Result.fail(msg, e)


# pylint: disable=too-few-public-methods
class Urllib3PollingRequester:
    """
    Urllib3PollingRequester is a PollingRequester that uses urllib3 to make HTTP requests.
    """

    def __init__(self, config):
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

        if len(query_params) > 0:
            filter_query = parse.urlencode(query_params)
            self._poll_uri += f"?{filter_query}"

        uri = self._poll_uri
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

    for event in data["events"]:
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
