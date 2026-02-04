"""
This module contains the implementations of a streaming synchronizer, along
with any required supporting classes and protocols.
"""

import json
from time import time
from typing import Callable, Generator, Optional, Tuple
from urllib import parse

from ld_eventsource import SSEClient
from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.config import (
    ConnectStrategy,
    ErrorStrategy,
    RetryDelayStrategy
)
from ld_eventsource.errors import HTTPStatusError

from ldclient.config import Config, DataSourceBuilder, HTTPConfig
from ldclient.impl.datasystem import DiagnosticAccumulator, DiagnosticSource
from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    PutObject
)
from ldclient.impl.http import HTTPFactory, _base_headers
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    Selector,
    SelectorStore,
    ServerIntent,
    Synchronizer,
    Update
)

# allows for up to 5 minutes to elapse without any data sent across the stream.
# The heartbeats sent as comments on the stream will keep this from triggering
STREAM_READ_TIMEOUT = 5 * 60

MAX_RETRY_DELAY = 30
BACKOFF_RESET_INTERVAL = 60
JITTER_RATIO = 0.5

STREAMING_ENDPOINT = "/sdk/stream"

SseClientBuilder = Callable[[str, HTTPConfig, float, Config, SelectorStore], SSEClient]


def create_sse_client(
    base_uri: str,
    http_options: HTTPConfig,
    initial_reconnect_delay: float,
    config: Config,
    ss: SelectorStore
) -> SSEClient:
    """ "
    create_sse_client creates an SSEClient instance configured to connect
    to the LaunchDarkly streaming endpoint.
    """
    uri = base_uri + STREAMING_ENDPOINT
    if config.payload_filter_key is not None:
        uri += "?%s" % parse.urlencode({"filter": config.payload_filter_key})

    # We don't want the stream to use the same read timeout as the rest of the SDK.
    base_headers = _base_headers(config)
    stream_http_factory = HTTPFactory(
        base_headers,
        http_options,
        override_read_timeout=STREAM_READ_TIMEOUT,
    )

    def query_params() -> dict[str, str]:
        selector = ss.selector()
        return {"basis": selector.state} if selector.is_defined() else {}

    return SSEClient(
        connect=ConnectStrategy.http(
            url=uri,
            headers=base_headers,
            pool=stream_http_factory.create_pool_manager(1, uri),
            urllib3_request_options={"timeout": stream_http_factory.timeout},
            query_params=query_params
        ),
        # we'll make error-handling decisions when we see a Fault
        error_strategy=ErrorStrategy.always_continue(),
        initial_retry_delay=initial_reconnect_delay,
        retry_delay_strategy=RetryDelayStrategy.default(
            max_delay=MAX_RETRY_DELAY,
            backoff_multiplier=2,
            jitter_multiplier=JITTER_RATIO,
        ),
        retry_delay_reset_threshold=BACKOFF_RESET_INTERVAL,
        logger=log,
    )


class StreamingDataSource(Synchronizer, DiagnosticSource):
    """
    StreamingSynchronizer is a specific type of Synchronizer that handles
    streaming data sources.

    It should implement the sync method to yield updates as they are received
    from the streaming data source.
    """

    def __init__(self,
                 uri: str,
                 http_options: HTTPConfig,
                 initial_reconnect_delay: float,
                 config: Config):
        self.__uri = uri
        self.__http_options = http_options
        self.__initial_reconnect_delay = initial_reconnect_delay

        self._sse_client_builder = create_sse_client
        self._config = config
        self._sse: Optional[SSEClient] = None
        self._running = False
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None
        self._connection_attempt_start_time: Optional[float] = None

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        self._diagnostic_accumulator = diagnostic_accumulator

    @property
    def name(self) -> str:
        """
        Returns the name of the synchronizer, which is used for logging and debugging.
        """
        return "streaming"

    def sync(self, ss: SelectorStore) -> Generator[Update, None, None]:
        """
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        self._sse = self._sse_client_builder(
            self.__uri,
            self.__http_options,
            self.__initial_reconnect_delay,
            self._config,
            ss
        )

        if self._sse is None:
            log.error("Failed to create SSE client for streaming updates.")
            return

        change_set_builder = ChangeSetBuilder()
        self._running = True
        self._connection_attempt_start_time = time()

        envid = None
        for action in self._sse.all:
            if isinstance(action, Fault):
                # If the SSE client detects the stream has closed, then it will
                # emit a fault with no-error. We can ignore this since we want
                # the connection to continue.
                if action.error is None:
                    continue

                if action.headers is not None:
                    envid = action.headers.get(_LD_ENVID_HEADER, envid)

                (update, should_continue) = self._handle_error(action.error, envid)
                if update is not None:
                    yield update

                if not should_continue:
                    break
                continue

            if isinstance(action, Start) and action.headers is not None:
                fallback = action.headers.get(_LD_FD_FALLBACK_HEADER) == 'true'
                envid = action.headers.get(_LD_ENVID_HEADER, envid)

                if fallback:
                    self._record_stream_init(True)
                    yield Update(
                        state=DataSourceState.OFF,
                        revert_to_fdv1=True,
                        environment_id=envid,
                    )
                    break

            if not isinstance(action, Event):
                continue

            try:
                update = self._process_message(action, change_set_builder, envid)
                if update is not None:
                    self._record_stream_init(False)
                    self._connection_attempt_start_time = None
                    yield update
            except json.decoder.JSONDecodeError as e:
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                self._sse.interrupt()

                (update, should_continue) = self._handle_error(e, envid)
                if update is not None:
                    yield update
                if not should_continue:
                    break
            except Exception as e:  # pylint: disable=broad-except
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                self._sse.interrupt()

                yield Update(
                    state=DataSourceState.INTERRUPTED,
                    error=DataSourceErrorInfo(
                        DataSourceErrorKind.UNKNOWN, 0, time(), str(e)
                    ),
                    revert_to_fdv1=False,
                    environment_id=envid,
                )

        self._sse.close()

    def stop(self):
        """
        Stops the streaming synchronizer, closing any open connections.
        """
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False
        if self._sse:
            self._sse.close()

    def _record_stream_init(self, failed: bool):
        if self._diagnostic_accumulator and self._connection_attempt_start_time:
            current_time = int(time() * 1000)
            elapsed = current_time - int(self._connection_attempt_start_time * 1000)
            self._diagnostic_accumulator.record_stream_init(current_time, elapsed if elapsed >= 0 else 0, failed)

    # pylint: disable=too-many-return-statements
    def _process_message(
        self, msg: Event, change_set_builder: ChangeSetBuilder, envid: Optional[str]
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
                    environment_id=envid,
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
                environment_id=envid,
            )

        log.info("Unexpected event found in stream: %s", msg.event)
        return None

    def _handle_error(self, error: Exception, envid: Optional[str]) -> Tuple[Optional[Update], bool]:
        """
        This method handles errors that occur during the streaming process.

        It may return an update indicating the error state, and a boolean
        indicating whether the synchronizer should continue retrying the connection.

        If an update is provided, it should be forward upstream, regardless of
        whether or not we are going to retry this failure.

        The return should be thought of (update, should_continue)
        """
        if not self._running:
            return (None, False)  # don't retry if we've been deliberately stopped

        update: Optional[Update] = None

        if isinstance(error, json.decoder.JSONDecodeError):
            log.error("Unexpected error on stream connection: %s, will retry", error)
            self._record_stream_init(True)
            self._connection_attempt_start_time = time() + \
                self._sse.next_retry_delay  # type: ignore

            update = Update(
                state=DataSourceState.INTERRUPTED,
                error=DataSourceErrorInfo(
                    DataSourceErrorKind.INVALID_DATA, 0, time(), str(error)
                ),
                revert_to_fdv1=False,
                environment_id=envid,
            )
            return (update, True)

        if isinstance(error, HTTPStatusError):
            self._record_stream_init(True)
            self._connection_attempt_start_time = time() + \
                self._sse.next_retry_delay  # type: ignore

            error_info = DataSourceErrorInfo(
                DataSourceErrorKind.ERROR_RESPONSE,
                error.status,
                time(),
                str(error),
            )

            if envid is None and error.headers is not None:
                envid = error.headers.get(_LD_ENVID_HEADER)

            if error.headers is not None and error.headers.get(_LD_FD_FALLBACK_HEADER) == 'true':
                update = Update(
                    state=DataSourceState.OFF,
                    error=error_info,
                    revert_to_fdv1=True,
                    environment_id=envid,
                )
                self.stop()
                return (update, False)

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
                environment_id=envid,
            )

            if not is_recoverable:
                self._connection_attempt_start_time = None
                log.error(http_error_message_result)
                self.stop()
                return (update, False)

            log.warning(http_error_message_result)
            return (update, True)

        log.warning("Unexpected error on stream connection: %s, will retry", error)
        self._record_stream_init(True)
        self._connection_attempt_start_time = time() + self._sse.next_retry_delay  # type: ignore

        update = Update(
            state=DataSourceState.INTERRUPTED,
            error=DataSourceErrorInfo(
                DataSourceErrorKind.UNKNOWN, 0, time(), str(error)
            ),
            revert_to_fdv1=False,
            environment_id=envid,
        )
        # no stacktrace here because, for a typical connection error, it'll
        # just be a lengthy tour of urllib3 internals

        return (update, True)


class StreamingDataSourceBuilder(DataSourceBuilder):
    """
    Builder for a StreamingDataSource.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__initial_reconnect_delay: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None

    def base_uri(self, uri: str) -> 'StreamingDataSourceBuilder':
        """Sets the base URI for the streaming data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def initial_reconnect_delay(self, delay: float) -> 'StreamingDataSourceBuilder':
        """Sets the initial reconnect delay for the streaming data source."""
        self.__initial_reconnect_delay = delay
        return self

    def http_options(self, http_options: HTTPConfig) -> 'StreamingDataSourceBuilder':
        """Sets the HTTP options for the streaming data source."""
        self.__http_options = http_options
        return self

    def build(self, config: Config) -> StreamingDataSource:
        """Builds a StreamingDataSource instance with the configured parameters."""
        return StreamingDataSource(
            self.__base_uri or config.stream_base_uri,
            self.__http_options or config.http,
            self.__initial_reconnect_delay or config.initial_reconnect_delay,
            config
        )
