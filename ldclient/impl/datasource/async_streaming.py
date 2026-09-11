"""
Default implementation of the streaming component.
"""

# currently excluded from documentation - see docs/README.md

import asyncio
import json
import time
from typing import Any, Callable, Optional
from urllib import parse

from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.errors import HTTPStatusError

from ldclient.impl.aio.concurrency import AsyncTaskRunner
from ldclient.impl.aio.transport import AsyncSSEFactory, make_client_session
from ldclient.impl.datasource.datasource_common import (
    STREAM_ALL_PATH,
    StreamClosedError,
    parse_path,
    sink_or_store
)
from ldclient.impl.retry import (
    FailureKind,
    RetryState,
    classify_http_status,
    for_streaming
)
from ldclient.impl.util import http_error_description, log
from ldclient.interfaces import (
    AsyncUpdateProcessor,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class AsyncStreamingUpdateProcessor(AsyncUpdateProcessor):
    """Reads flag data from LaunchDarkly's streaming endpoint on a background task.

    The SDK owns the delay between connection attempts rather than the SSE
    client; see :mod:`ldclient.impl.retry`.
    """

    def __init__(self, config, store, ready, diagnostic_accumulator, sse_factory: Optional[AsyncSSEFactory] = None, retry_state: Optional[RetryState] = None):
        self._uri = config.stream_base_uri + STREAM_ALL_PATH
        if config.payload_filter_key is not None:
            self._uri += '?%s' % parse.urlencode({'filter': config.payload_filter_key})
        self._config = config
        self._data_source_update_sink = config.data_source_update_sink
        self._store = store
        self._running = False
        self._ready = ready
        self._diagnostic_accumulator = diagnostic_accumulator
        # A caller-supplied factory owns whatever session it uses. With no
        # factory we build our own session + factory, but that is deferred to
        # _run() so the aiohttp ClientSession is created on the running event
        # loop rather than at construction time.
        self._sse_factory = sse_factory
        self._owned_session = None
        self._sse: Any = None
        self._connection_attempt_start_time: Optional[float] = None
        self._runner = AsyncTaskRunner()
        self._started = False
        self._retry = retry_state or for_streaming(config.initial_reconnect_delay)
        self._interrupted_by_sdk = False

    def start(self):
        if self._started:
            log.info("AsyncStreamingUpdateProcessor has already been started; ignoring")
            return
        self._started = True
        self._runner.spawn("ldclient.datasource.streaming", self._run)

    async def _run(self):
        if self._sse_factory is None:
            # Build a session from the SDK's HTTP options (CA certs, client cert,
            # SSL verification, proxy trust). Build it here, on the running loop,
            # as aiohttp requires. The SSE client does not close a supplied
            # session, so this data source closes it on teardown.
            self._owned_session = make_client_session(self._config)
            self._sse_factory = AsyncSSEFactory(self._config, session=self._owned_session)
        log.info("Starting AsyncStreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        try:
            self._sse = self._sse_factory.create(self._uri, self._config.initial_reconnect_delay, sdk_managed_retry=True)
            self._connection_attempt_start_time = time.time()
            async for action in self._sse.all:
                if isinstance(action, Start):
                    # On reconnect after an error the timer was cleared; reset it here.
                    # For the initial connect the pre-loop timestamp is already set.
                    if self._connection_attempt_start_time is None:
                        self._connection_attempt_start_time = time.time()
                elif isinstance(action, Event):
                    message_ok = False
                    message_handled = False
                    try:
                        message_ok = await self._process_message(action)
                        message_handled = True
                    except json.decoder.JSONDecodeError as e:
                        log.info("Error while handling stream event; will restart stream: %s" % e)
                        await self._interrupt_stream()

                        if not await self._handle_error(e):
                            break
                    except Exception as e:
                        log.warning("Error while handling stream event; will restart stream: %s" % e)
                        await self._interrupt_stream()

                        if not await self._handle_error(e):
                            break

                    if message_handled:
                        self._retry.record_success()

                    if message_ok:
                        self._record_stream_init(False)
                        self._connection_attempt_start_time = None

                        if self._data_source_update_sink is not None:
                            self._data_source_update_sink.update_status(DataSourceState.VALID, None)

                        if not self._ready.is_set():
                            log.info("AsyncStreamingUpdateProcessor initialized ok.")
                            self._ready.set()
                elif isinstance(action, Fault):
                    # A Fault with no error means the connection closed cleanly.
                    # If we asked for that close, we have already recorded the
                    # failure behind it and must not record it twice. Otherwise
                    # the server closed a connection it normally leaves open,
                    # which is a connection failure the SDK backs off from.
                    if action.error is None:
                        if self._interrupted_by_sdk:
                            self._interrupted_by_sdk = False
                            continue
                        if not await self._handle_error(StreamClosedError()):
                            break
                        continue

                    if not await self._handle_error(action.error):
                        break
        finally:
            if self._sse:
                try:
                    await self._sse.close()
                except Exception as e:
                    log.warning("Error closing stream connection during shutdown: %s" % e)
            await self._close_owned_session()

    async def _close_owned_session(self):
        """Close the aiohttp session if the SDK created it. A caller-supplied
        factory owns its own session, so ``_owned_session`` is ``None`` and
        nothing is closed here. Closing resets the reference to ``None`` so it
        isn't closed twice."""
        if self._owned_session is not None:
            await self._owned_session.close()
            self._owned_session = None

    def _record_stream_init(self, failed: bool):
        if self._diagnostic_accumulator and self._connection_attempt_start_time:
            current_time = int(time.time() * 1000)
            elapsed = current_time - int(self._connection_attempt_start_time * 1000)
            self._diagnostic_accumulator.record_stream_init(current_time, elapsed if elapsed >= 0 else 0, failed)

    async def stop(self):
        # Cancel the run task first: otherwise, if stop() is called before _run has executed, the
        # loop could run _run at the teardown await and create a fresh SSE connection against the
        # session we're closing. Once the runner is stopped, teardown is safe.
        await self._runner.stop_all()

        log.info("Stopping AsyncStreamingUpdateProcessor")
        self._running = False
        if self._sse:
            await self._sse.close()
        await self._close_owned_session()

        if self._data_source_update_sink is None:
            return

        # OFF means an explicit shutdown. No stream failure produces it.
        self._data_source_update_sink.update_status(DataSourceState.OFF, None)

    async def _interrupt_stream(self):
        """Drops the stream connection so the next read reconnects. The SSE
        client reports the close as a Fault with no error, and the flag tells
        the loop that this one is ours and is already accounted for."""
        self._interrupted_by_sdk = True
        await self._sse.interrupt()

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    # Returns True if we initialized the feature store
    async def _process_message(self, msg: Event) -> bool:
        """Process a single SSE event.  Returns True on a successful ``put``."""
        target = sink_or_store(self._data_source_update_sink, self._store)
        if msg.event == 'put':
            all_data = json.loads(msg.data)
            init_data = {FEATURES: all_data['data']['flags'], SEGMENTS: all_data['data']['segments']}
            log.debug("Received put event with %d flags and %d segments", len(init_data[FEATURES]), len(init_data[SEGMENTS]))
            await target.init(init_data)
            return True
        elif msg.event == 'patch':
            payload = json.loads(msg.data)
            path = payload['path']
            obj = payload['data']
            log.debug("Received patch event for %s, New version: [%d]", path, obj.get("version"))
            parsed = parse_path(path)
            if parsed is not None:
                await target.upsert(parsed.kind, obj)
            else:
                log.warning("Patch for unknown path")
        elif msg.event == 'delete':
            payload = json.loads(msg.data)
            path = payload['path']
            # noinspection PyShadowingNames
            version = payload['version']
            log.debug("Received delete event for %s, New version: [%d]", path, version)
            parsed = parse_path(path)
            if parsed is not None:
                await target.delete(parsed.kind, parsed.key, version)
            else:
                log.warning("Delete for unknown path")
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)
        return False

    # Returns true to continue, false to stop
    async def _handle_error(self, error: Exception) -> bool:
        """Records a stream failure, reports it, and waits before the retry.

        Returns True once the wait is over, or False if the processor was
        stopped. No failure ever ends the stream by itself. The wait is
        interrupted by cancelling the task, which matters because the extended
        regime can ask for an hour.
        """
        if not self._running:
            return False  # don't retry if we've been deliberately stopped

        self._record_stream_init(True)

        level: Callable[..., None]

        if isinstance(error, json.decoder.JSONDecodeError):
            kind = FailureKind.NORMAL
            error_info = DataSourceErrorInfo(DataSourceErrorKind.INVALID_DATA, 0, time.time(), str(error))
            description = "Unparseable data on stream connection: %s" % error
            level = log.error
        elif isinstance(error, HTTPStatusError):
            kind = classify_http_status(error.status)
            error_info = DataSourceErrorInfo(DataSourceErrorKind.ERROR_RESPONSE, error.status, time.time(), str(error))
            description = "Received %s for stream connection" % http_error_description(error.status)
            level = log.error if kind is FailureKind.UNEXPECTED else log.warning
        elif isinstance(error, StreamClosedError):
            kind = FailureKind.NORMAL
            error_info = DataSourceErrorInfo(DataSourceErrorKind.NETWORK_ERROR, 0, time.time(), str(error))
            description = "The server closed the stream connection"
            level = log.warning
        else:
            # A certificate failure lands here too, and is as normal as the rest.
            kind = FailureKind.NORMAL
            error_info = DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(error))
            # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of HTTP client internals
            description = "Error on stream connection: %s" % error
            level = log.warning

        delay = self._retry.record_failure(kind)
        level("%s - will retry in %.1fs" % (description, delay))

        if self._data_source_update_sink is not None:
            self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)

        self._connection_attempt_start_time = time.time() + delay
        if delay > 0:
            await asyncio.sleep(delay)
        return self._running

    # magic methods for "with" statement (used in testing)
    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        await self.stop()
