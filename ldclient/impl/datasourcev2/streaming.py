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

from ldclient.config import (
    DataSourceBuilder,
    DataSourceBuilderConfig,
    HTTPConfig
)
from ldclient.impl.datasourcev2.streaming_common import (
    classify_stream_error,
    process_message,
    with_fallback_signal
)
from ldclient.impl.datasystem import DiagnosticAccumulator, DiagnosticSource
from ldclient.impl.http import HTTPFactory, _base_headers
from ldclient.impl.util import _LD_ENVID_HEADER, _LD_FD_FALLBACK_HEADER, log
from ldclient.interfaces import (
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    SelectorStore,
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

SseClientBuilder = Callable[
    [str, HTTPConfig, float, DataSourceBuilderConfig, SelectorStore],
    SSEClient,
]


def create_sse_client(
    base_uri: str,
    http_options: HTTPConfig,
    initial_reconnect_delay: float,
    config: DataSourceBuilderConfig,
    ss: SelectorStore
) -> SSEClient:
    """
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
                 config: DataSourceBuilderConfig):
        self.__uri = uri
        self.__http_options = http_options
        self.__initial_reconnect_delay = initial_reconnect_delay

        self._sse_client_builder: SseClientBuilder = create_sse_client
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
        # fallback_requested is set when a Start action carries
        # X-LD-FD-Fallback: true. We finish applying the current payload
        # before halting, so consumers can serve the server-provided data
        # while FDv1 takes over. The latch is one-way and terminal: once
        # set, any subsequent payload-completing event or error must carry
        # the signal forward and halt the stream, even if the failure path
        # itself doesn't see the directive header.
        fallback_requested = False

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
                    yield with_fallback_signal(update, fallback_requested)

                # The FDv1 Fallback Directive is one-way and terminal: if it
                # was latched on a prior Start, we must not keep retrying the
                # FDv2 endpoint even when the failure itself looks recoverable.
                if fallback_requested or not should_continue:
                    break
                continue

            if isinstance(action, Start) and action.headers is not None:
                envid = action.headers.get(_LD_ENVID_HEADER, envid)
                if action.headers.get(_LD_FD_FALLBACK_HEADER) == 'true':
                    fallback_requested = True

            if not isinstance(action, Event):
                continue

            try:
                update = process_message(action, change_set_builder, envid)
                if update is not None:
                    self._record_stream_init(False)
                    self._connection_attempt_start_time = None
                    if fallback_requested:
                        # The completed update is the natural moment to honor
                        # the latched directive: yield once with the signal,
                        # then halt — the consumer will switch to FDv1.
                        yield with_fallback_signal(update, fallback_requested)
                        break
                    yield update
            except json.decoder.JSONDecodeError as e:
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                self._sse.interrupt()

                (update, should_continue) = self._handle_error(e, envid)
                if update is not None:
                    yield with_fallback_signal(update, fallback_requested)
                if fallback_requested or not should_continue:
                    break
            except Exception as e:  # pylint: disable=broad-except
                log.info(
                    "Error while handling stream event; will restart stream: %s", e
                )
                self._sse.interrupt()

                yield with_fallback_signal(Update(
                    state=DataSourceState.INTERRUPTED,
                    error=DataSourceErrorInfo(
                        DataSourceErrorKind.UNKNOWN, 0, time(), str(e)
                    ),
                    fallback_to_fdv1=False,
                    environment_id=envid,
                ), fallback_requested)
                if fallback_requested:
                    break

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

        decision = classify_stream_error(
            error, self._sse.next_retry_delay, envid  # type: ignore
        )
        self._record_stream_init(True)
        self._connection_attempt_start_time = decision.next_attempt_start
        if decision.should_stop:
            self.stop()

        return (decision.update, decision.should_continue)


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

    def build(self, config: DataSourceBuilderConfig) -> StreamingDataSource:
        """Builds a StreamingDataSource instance with the configured parameters."""
        return StreamingDataSource(
            self.__base_uri or config.stream_base_uri,
            self.__http_options or config.http,
            self.__initial_reconnect_delay or config.initial_reconnect_delay,
            config
        )
