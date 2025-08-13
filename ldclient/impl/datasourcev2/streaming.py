"""
This module contains the implementations of a streaming synchronizer, along
with any required supporting classes and protocols.
"""

import json
from abc import abstractmethod
from time import time
from typing import Callable, Generator, Iterable, Optional, Protocol, Tuple
from urllib import parse

from ld_eventsource import SSEClient as SSEClientImpl
from ld_eventsource.actions import Action, Event, Fault
from ld_eventsource.config import (
    ConnectStrategy,
    ErrorStrategy,
    RetryDelayStrategy
)
from ld_eventsource.errors import HTTPStatusError

from ldclient.config import Config
from ldclient.impl.datasystem import Synchronizer, Update
from ldclient.impl.datasystem.protocolv2 import (
    ChangeSetBuilder,
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    IntentCode,
    PutObject,
    Selector,
    ServerIntent
)
from ldclient.impl.http import HTTPFactory, _http_factory
from ldclient.impl.util import (
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState
)

# allows for up to 5 minutes to elapse without any data sent across the stream.
# The heartbeats sent as comments on the stream will keep this from triggering
STREAM_READ_TIMEOUT = 5 * 60

MAX_RETRY_DELAY = 30
BACKOFF_RESET_INTERVAL = 60
JITTER_RATIO = 0.5

STREAMING_ENDPOINT = "/sdk/stream"


class SSEClient(Protocol):  # pylint: disable=too-few-public-methods
    """
    SSEClient is a protocol that defines the interface for a client that can
    connect to a Server-Sent Events (SSE) stream and provide an iterable of
    actions received from that stream.
    """

    @property
    @abstractmethod
    def all(self) -> Iterable[Action]:
        """
        Returns an iterable of all actions received from the SSE stream.
        """
        raise NotImplementedError


SseClientBuilder = Callable[[Config], SSEClient]


# TODO(sdk-1391): Pass a selector-retrieving function through so it can
# re-connect with the last known status.
def create_sse_client(config: Config) -> SSEClientImpl:
    """ "
    create_sse_client creates an SSEClientImpl instance configured to connect
    to the LaunchDarkly streaming endpoint.
    """
    uri = config.stream_base_uri + STREAMING_ENDPOINT

    # We don't want the stream to use the same read timeout as the rest of the SDK.
    http_factory = _http_factory(config)
    stream_http_factory = HTTPFactory(
        http_factory.base_headers,
        http_factory.http_config,
        override_read_timeout=STREAM_READ_TIMEOUT,
    )

    return SSEClientImpl(
        connect=ConnectStrategy.http(
            url=uri,
            headers=http_factory.base_headers,
            pool=stream_http_factory.create_pool_manager(1, uri),
            urllib3_request_options={"timeout": stream_http_factory.timeout},
        ),
        # we'll make error-handling decisions when we see a Fault
        error_strategy=ErrorStrategy.always_continue(),
        initial_retry_delay=config.initial_reconnect_delay,
        retry_delay_strategy=RetryDelayStrategy.default(
            max_delay=MAX_RETRY_DELAY,
            backoff_multiplier=2,
            jitter_multiplier=JITTER_RATIO,
        ),
        retry_delay_reset_threshold=BACKOFF_RESET_INTERVAL,
        logger=log,
    )


class StreamingDataSource(Synchronizer):
    """
    StreamingSynchronizer is a specific type of Synchronizer that handles
    streaming data sources.

    It should implement the sync method to yield updates as they are received
    from the streaming data source.
    """

    def __init__(
        self, config: Config, sse_client_builder: SseClientBuilder = create_sse_client
    ):
        self._sse_client_builder = sse_client_builder
        self._uri = config.stream_base_uri + STREAMING_ENDPOINT
        if config.payload_filter_key is not None:
            self._uri += "?%s" % parse.urlencode({"filter": config.payload_filter_key})
        self._config = config
        self._sse: Optional[SSEClient] = None

    def sync(self) -> Generator[Update, None, None]:
        """
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        log.info("Starting StreamingUpdateProcessor connecting to uri: %s", self._uri)
        self._sse = self._sse_client_builder(self._config)
        if self._sse is None:
            log.error("Failed to create SSE client for streaming updates.")
            return

        change_set_builder = ChangeSetBuilder()

        for action in self._sse.all:
            if isinstance(action, Fault):
                # If the SSE client detects the stream has closed, then it will
                # emit a fault with no-error. We can ignore this since we want
                # the connection to continue.
                if action.error is None:
                    continue

                (update, should_continue) = self._handle_error(action.error)
                if update is not None:
                    yield update

                if not should_continue:
                    break
                continue

            if not isinstance(action, Event):
                continue

            try:
                update = self._process_message(action, change_set_builder)
                if update is not None:
                    yield update
            except json.decoder.JSONDecodeError as e:
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                # TODO(sdk-1409)
                # self._sse.interrupt()

                (update, should_continue) = self._handle_error(e)
                if update is not None:
                    yield update
                if not should_continue:
                    break
            except Exception as e:  # pylint: disable=broad-except
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                # TODO(sdk-1409)
                # self._sse.interrupt()

                yield Update(
                    state=DataSourceState.INTERRUPTED,
                    error=DataSourceErrorInfo(
                        DataSourceErrorKind.UNKNOWN, 0, time(), str(e)
                    ),
                    revert_to_fdv1=False,
                    environment_id=None,  # TODO(sdk-1410)
                )

            # TODO(sdk-1408)
            # if update is not None:
            #     self._record_stream_init(False)

            # if self._data_source_update_sink is not None:
            #     self._data_source_update_sink.update_status(
            #         DataSourceState.VALID, None
            #     )

            # if not self._ready.is_set():
            #     log.info("StreamingUpdateProcessor initialized ok.")
            #     self._ready.set()

        # TODO(sdk-1409)
        # self._sse.close()

    # TODO(sdk-1409)
    # def stop(self):
    #     self.__stop_with_error_info(None)
    #
    # def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
    #     log.info("Stopping StreamingUpdateProcessor")
    #     self._running = False
    #     if self._sse:
    #         self._sse.close()
    #
    #     if self._data_source_update_sink is None:
    #         return
    #
    #     self._data_source_update_sink.update_status(DataSourceState.OFF, error)

    # pylint: disable=too-many-return-statements
    def _process_message(
        self, msg: Event, change_set_builder: ChangeSetBuilder
    ) -> Optional[Update]:
        """
        Processes a single message from the SSE stream and returns an Update
        object if applicable.

        This method may raise exceptions if the message is malformed or if an
        error occurs while processing the message. The caller should handle these
        exceptions appropriately.
        """
        if msg.event == EventName.HEARTBEAT:
            return None

        if msg.event == EventName.SERVER_INTENT:
            server_intent = ServerIntent.from_dict(json.loads(msg.data))
            change_set_builder.start(server_intent.payload.code)

            if server_intent.payload.code == IntentCode.TRANSFER_NONE:
                change_set_builder.expect_changes()
                return Update(
                    state=DataSourceState.VALID,
                    environment_id=None,  # TODO(sdk-1410)
                )
            return None

        if msg.event == EventName.PUT_OBJECT:
            put = PutObject.from_dict(json.loads(msg.data))
            change_set_builder.add_put(put.kind, put.key, put.version, put.object)
            return None

        if msg.event == EventName.DELETE_OBJECT:
            delete = DeleteObject.from_dict(json.loads(msg.data))
            change_set_builder.add_delete(delete.kind, delete.key, delete.version)
            return None

        if msg.event == EventName.GOODBYE:
            goodbye = Goodbye.from_dict(json.loads(msg.data))
            if not goodbye.silent:
                log.error(
                    "SSE server received error: %s (%s)",
                    goodbye.reason,
                    goodbye.catastrophe,
                )

            return None

        if msg.event == EventName.ERROR:
            error = Error.from_dict(json.loads(msg.data))
            log.error("Error on %s: %s", error.payload_id, error.reason)

            # The protocol should "reset" any previous change events it has
            # received, but should continue to operate under the assumption the
            # last server intent was in effect.
            #
            # The server may choose to send a new server-intent, at which point
            # we will set that as well.
            change_set_builder.reset()

            return None

        if msg.event == EventName.PAYLOAD_TRANSFERRED:
            selector = Selector.from_dict(json.loads(msg.data))
            change_set = change_set_builder.finish(selector)

            return Update(
                state=DataSourceState.VALID,
                change_set=change_set,
                environment_id=None,  # TODO(sdk-1410)
            )

        log.info("Unexpected event found in stream: %s", msg.event)
        return None

    def _handle_error(self, error: Exception) -> Tuple[Optional[Update], bool]:
        """
        This method handles errors that occur during the streaming process.

        It may return an update indicating the error state, and a boolean
        indicating whether the synchronizer should continue retrying the connection.

        If an update is provided, it should be forward upstream, regardless of
        whether or not we are going to retry this failure.
        """
        # if not self._running:
        #     return (False, None)  # don't retry if we've been deliberately stopped

        update: Optional[Update] = None

        if isinstance(error, json.decoder.JSONDecodeError):
            log.error("Unexpected error on stream connection: %s, will retry", error)

            update = Update(
                state=DataSourceState.INTERRUPTED,
                error=DataSourceErrorInfo(
                    DataSourceErrorKind.INVALID_DATA, 0, time(), str(error)
                ),
                revert_to_fdv1=False,
                environment_id=None,  # TODO(sdk-1410)
            )
            return (update, True)

        if isinstance(error, HTTPStatusError):
            error_info = DataSourceErrorInfo(
                DataSourceErrorKind.ERROR_RESPONSE,
                error.status,
                time(),
                str(error),
            )

            http_error_message_result = http_error_message(
                error.status, "stream connection"
            )

            is_recoverable = is_http_error_recoverable(error.status)

            update = Update(
                state=(
                    DataSourceState.INTERRUPTED
                    if is_recoverable
                    else DataSourceState.OFF
                ),
                error=error_info,
                revert_to_fdv1=False,
                environment_id=None,  # TODO(sdk-1410)
            )

            if not is_recoverable:
                log.error(http_error_message_result)
                # TODO(sdk-1409)
                # self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                # self.__stop_with_error_info(error_info)
                # self.stop()
                return (update, False)

            log.warning(http_error_message_result)
            return (update, True)

        log.warning("Unexpected error on stream connection: %s, will retry", error)

        update = Update(
            state=DataSourceState.INTERRUPTED,
            error=DataSourceErrorInfo(
                DataSourceErrorKind.UNKNOWN, 0, time(), str(error)
            ),
            revert_to_fdv1=False,
            environment_id=None,  # TODO(sdk-1410)
        )
        # no stacktrace here because, for a typical connection error, it'll
        # just be a lengthy tour of urllib3 internals

        return (update, True)

    # magic methods for "with" statement (used in testing)
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # self.stop()
        pass


class StreamingDataSourceBuilder:  # disable: pylint: disable=too-few-public-methods
    """
    Builder for a StreamingDataSource.
    """

    def __init__(self, config: Config):
        self._config = config

    def build(self) -> StreamingDataSource:
        """Builds a StreamingDataSource instance with the configured parameters."""
        # TODO(fdv2): Add in the other controls here.
        return StreamingDataSource(self._config)
