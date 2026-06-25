"""
This module contains the implementations of a streaming synchronizer, along
with any required supporting classes and protocols.
"""

import json
from time import time
from typing import AsyncGenerator, Callable, Optional, Tuple
from urllib import parse

import aiohttp
from ld_eventsource import AsyncSSEClient
from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.errors import HTTPStatusError

from ldclient.config import (
    DataSourceBuilder,
    DataSourceBuilderConfig,
    HTTPConfig
)
from ldclient.impl.aio.transport import AsyncSSEFactory, make_client_session
from ldclient.impl.datasourcev2.streaming_common import process_message
from ldclient.impl.datasystem import DiagnosticAccumulator, DiagnosticSource
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    AsyncSynchronizer,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    SelectorStore,
    Update
)

STREAMING_ENDPOINT = "/sdk/stream"

SseClientBuilder = Callable[
    [str, HTTPConfig, float, DataSourceBuilderConfig, SelectorStore],
    Tuple[AsyncSSEClient, Optional[aiohttp.ClientSession]],
]


def create_sse_client(
    base_uri: str,
    http_options: HTTPConfig,
    initial_reconnect_delay: float,
    config: DataSourceBuilderConfig,
    ss: SelectorStore,
    session=None,
) -> Tuple[AsyncSSEClient, Optional[aiohttp.ClientSession]]:
    """
    create_sse_client creates an SSE client configured to connect to the
    LaunchDarkly streaming endpoint, along with the aiohttp session backing it
    when the SDK created that session itself.

    When no ``session`` is supplied, one is built from the SDK's HTTP options
    via ``make_client_session`` (CA certs, client cert, SSL verification, proxy
    trust, connector limits) and returned as the second element so the caller
    can close it on shutdown -- the SSE client treats the supplied session as
    externally owned and never closes it. When a ``session`` is supplied, the
    caller owns it and ``None`` is returned in its place.
    """
    uri = base_uri + STREAMING_ENDPOINT
    if config.payload_filter_key is not None:
        uri += "?%s" % parse.urlencode({"filter": config.payload_filter_key})

    def query_params() -> dict:
        selector = ss.selector()
        return {"basis": selector.state} if selector.is_defined() else {}

    if session is None:
        session = make_client_session(config, http_options)
        owned_session: Optional[aiohttp.ClientSession] = session
    else:
        owned_session = None

    factory = AsyncSSEFactory(
        config,
        session=session,
        http_options=http_options,
    )
    sse_client = factory.create(uri, initial_reconnect_delay, query_params=query_params)
    return sse_client, owned_session


class AsyncStreamingDataSource(AsyncSynchronizer, DiagnosticSource):
    """
    AsyncStreamingDataSource is a specific type of synchronizer that handles
    streaming data sources.

    It should implement the sync method to yield updates as they are received
    from the streaming data source.
    """

    def __init__(
        self,
        uri: str,
        http_options: HTTPConfig,
        initial_reconnect_delay: float,
        config: DataSourceBuilderConfig,
        session=None,
    ):
        self.__uri = uri
        self.__http_options = http_options
        self.__initial_reconnect_delay = initial_reconnect_delay

        self._sse_client_builder: SseClientBuilder = create_sse_client
        self._config = config
        self._session = session
        # Late-bound default builder so the ctor-provided session reaches the SSE client.
        self._sse_client_builder = lambda *args: create_sse_client(*args, session=self._session)  # type: ignore[misc]
        self._sse: Optional[AsyncSSEClient] = None
        self._owned_session: Optional[aiohttp.ClientSession] = None
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

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        """
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        self._sse, self._owned_session = self._sse_client_builder(
            self.__uri,
            self.__http_options,
            self.__initial_reconnect_delay,
            self._config,
            ss
        )

        if self._sse is None:
            log.error("Failed to create SSE client for streaming updates.")
            await self._close_owned_session()
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

        def _with_fallback_signal(update: Update) -> Update:
            """Return ``update`` decorated with ``fallback_to_fdv1=True`` when
            the directive has been latched. Idempotent if already set."""
            if not fallback_requested or update.fallback_to_fdv1:
                return update
            return Update(
                state=update.state,
                change_set=update.change_set,
                error=update.error,
                fallback_to_fdv1=True,
                environment_id=update.environment_id,
            )

        try:
            async for action in self._sse.all:
                if isinstance(action, Fault):
                    # If the SSE client detects the stream has closed, then it will
                    # emit a fault with no-error. We can ignore this since we want
                    # the connection to continue.
                    if action.error is None:
                        continue

                    if action.headers is not None:
                        envid = action.headers.get(_LD_ENVID_HEADER, envid)

                    (update, should_continue) = await self._handle_error(action.error, envid)
                    if update is not None:
                        yield _with_fallback_signal(update)

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
                            yield _with_fallback_signal(update)
                            break
                        yield update
                except json.decoder.JSONDecodeError as e:
                    log.info(
                        "Error while handling stream event; will restart stream: %s", e
                    )
                    await self._sse.interrupt()

                    (update, should_continue) = await self._handle_error(e, envid)
                    if update is not None:
                        yield _with_fallback_signal(update)
                    if fallback_requested or not should_continue:
                        break
                except Exception as e:  # pylint: disable=broad-except
                    log.info(
                        "Error while handling stream event; will restart stream: %s", e
                    )
                    await self._sse.interrupt()

                    yield _with_fallback_signal(Update(
                        state=DataSourceState.INTERRUPTED,
                        error=DataSourceErrorInfo(
                            DataSourceErrorKind.UNKNOWN, 0, time(), str(e)
                        ),
                        fallback_to_fdv1=False,
                        environment_id=envid,
                    ))
                    if fallback_requested:
                        break
        finally:
            await self._sse.close()
            await self._close_owned_session()

    async def stop(self):
        """
        Stops the streaming synchronizer, closing any open connections.
        """
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False
        if self._sse:
            await self._sse.close()
        await self._close_owned_session()

    async def _close_owned_session(self):
        """Close the aiohttp session if the SDK created it. A caller-supplied
        session is owned by the caller and is never closed here. Closing sets
        the reference back to ``None`` so it isn't closed twice."""
        if self._owned_session is not None:
            await self._owned_session.close()
            self._owned_session = None

    def _record_stream_init(self, failed: bool):
        if self._diagnostic_accumulator and self._connection_attempt_start_time:
            current_time = int(time() * 1000)
            elapsed = current_time - int(self._connection_attempt_start_time * 1000)
            self._diagnostic_accumulator.record_stream_init(current_time, elapsed if elapsed >= 0 else 0, failed)

    async def _handle_error(self, error: Exception, envid: Optional[str]) -> Tuple[Optional[Update], bool]:
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
                fallback_to_fdv1=False,
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
                    fallback_to_fdv1=True,
                    environment_id=envid,
                )
                await self.stop()
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
                fallback_to_fdv1=False,
                environment_id=envid,
            )

            if not is_recoverable:
                self._connection_attempt_start_time = None
                log.error(http_error_message_result)
                await self.stop()
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
            fallback_to_fdv1=False,
            environment_id=envid,
        )
        # no stacktrace here because, for a typical connection error, it'll
        # just be a lengthy tour of HTTP client internals

        return (update, True)


class AsyncStreamingDataSourceBuilder(DataSourceBuilder):
    """
    Builder for a AsyncStreamingDataSource.
    """

    def __init__(self):
        self.__base_uri: Optional[str] = None
        self.__initial_reconnect_delay: Optional[float] = None
        self.__http_options: Optional[HTTPConfig] = None
        self.__session = None

    def base_uri(self, uri: str) -> 'AsyncStreamingDataSourceBuilder':
        """Sets the base URI for the streaming data source."""
        self.__base_uri = uri.rstrip('/')
        return self

    def initial_reconnect_delay(self, delay: float) -> 'AsyncStreamingDataSourceBuilder':
        """Sets the initial reconnect delay for the streaming data source."""
        self.__initial_reconnect_delay = delay
        return self

    def http_options(self, http_options: HTTPConfig) -> 'AsyncStreamingDataSourceBuilder':
        """Sets the HTTP options for the streaming data source."""
        self.__http_options = http_options
        return self

    def session(self, session) -> 'AsyncStreamingDataSourceBuilder':
        """Sets the aiohttp session for the streaming data source."""
        self.__session = session
        return self

    def build(self, config: DataSourceBuilderConfig) -> AsyncStreamingDataSource:
        """Builds a AsyncStreamingDataSource instance with the configured parameters."""
        return AsyncStreamingDataSource(
            self.__base_uri or config.stream_base_uri,
            self.__http_options or config.http,
            self.__initial_reconnect_delay or config.initial_reconnect_delay,
            config,
            session=self.__session,
        )
