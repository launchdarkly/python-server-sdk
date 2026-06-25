"""
Default implementation of the streaming component.
"""

# currently excluded from documentation - see docs/README.md

import json
import time
from typing import Any, Optional
from urllib import parse

from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.errors import HTTPStatusError

from ldclient.impl.aio.concurrency import AsyncTaskRunner
from ldclient.impl.aio.transport import AsyncSSEFactory, make_client_session
from ldclient.impl.datasource.datasource_common import (
    STREAM_ALL_PATH,
    parse_path,
    sink_or_store
)
from ldclient.impl.util import (
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    UpdateProcessor
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class AsyncStreamingUpdateProcessor(UpdateProcessor):
    def __init__(self, config, store, ready, diagnostic_accumulator, sse_factory: Optional[AsyncSSEFactory] = None):
        self._uri = config.stream_base_uri + STREAM_ALL_PATH
        if config.payload_filter_key is not None:
            self._uri += '?%s' % parse.urlencode({'filter': config.payload_filter_key})
        self._config = config
        self._data_source_update_sink = config.data_source_update_sink
        self._store = store
        self._running = False
        self._ready = ready
        self._diagnostic_accumulator = diagnostic_accumulator
        if sse_factory is not None:
            # A caller-supplied factory owns whatever session it uses; we don't
            # create or close one here.
            self._sse_factory = sse_factory
            self._owned_session = None
        else:
            # Build a session configured from the SDK's HTTP options (CA certs,
            # client cert, SSL verification, proxy trust) so the streaming
            # connection isn't a plain unconfigured ClientSession. The SSE
            # client treats the supplied session as externally owned and never
            # closes it, so this data source closes it on teardown.
            self._owned_session = make_client_session(config)
            self._sse_factory = AsyncSSEFactory(config, session=self._owned_session)
        self._sse: Any = None
        self._connection_attempt_start_time = None
        self._runner = AsyncTaskRunner()
        self._started = False

    def start(self):
        if self._started:
            raise RuntimeError("processors can only be started once")
        self._started = True
        self._runner.spawn("ldclient.datasource.streaming", self._run)

    async def _run(self):
        log.info("Starting AsyncStreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        self._sse = self._sse_factory.create(self._uri, self._config.initial_reconnect_delay)
        self._connection_attempt_start_time = time.time()
        async for action in self._sse.all:
            if isinstance(action, Start):
                # On reconnect after an error the timer was cleared; reset it here.
                # For the initial connect the pre-loop timestamp is already set.
                if self._connection_attempt_start_time is None:
                    self._connection_attempt_start_time = time.time()
            elif isinstance(action, Event):
                message_ok = False
                try:
                    message_ok = await self._process_message(action)
                except json.decoder.JSONDecodeError as e:
                    log.info("Error while handling stream event; will restart stream: %s" % e)
                    await self._sse.interrupt()

                    await self._handle_error(e)
                except Exception as e:
                    log.warning("Error while handling stream event; will restart stream: %s" % e)
                    await self._sse.interrupt()

                    if self._data_source_update_sink is not None:
                        error_info = DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(e))

                        self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)

                if message_ok:
                    self._record_stream_init(False)
                    self._connection_attempt_start_time = None

                    if self._data_source_update_sink is not None:
                        self._data_source_update_sink.update_status(DataSourceState.VALID, None)

                    if not self._ready.is_set():
                        log.info("AsyncStreamingUpdateProcessor initialized ok.")
                        self._ready.set()
            elif isinstance(action, Fault):
                # If the SSE client detects the stream has closed, then it will emit a fault with no-error. We can
                # ignore this since we want the connection to continue.
                if action.error is None:
                    continue

                if not await self._handle_error(action.error):
                    break
        await self._sse.close()
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
        await self.__stop_with_error_info(None)
        await self._runner.stop_all()

    async def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
        log.info("Stopping AsyncStreamingUpdateProcessor")
        self._running = False
        if self._sse:
            await self._sse.close()
        await self._close_owned_session()

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(DataSourceState.OFF, error)

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
                log.warning("Patch for unknown path: %s", path)
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
                log.warning("Delete for unknown path: %s", path)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)
        return False

    # Returns true to continue, false to stop
    async def _handle_error(self, error: Exception) -> bool:
        if not self._running:
            return False  # don't retry if we've been deliberately stopped

        if isinstance(error, json.decoder.JSONDecodeError):
            error_info = DataSourceErrorInfo(DataSourceErrorKind.INVALID_DATA, 0, time.time(), str(error))

            log.error("Unexpected error on stream connection: %s, will retry" % error)
            self._record_stream_init(True)
            self._connection_attempt_start_time = None

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)
        elif isinstance(error, HTTPStatusError):
            self._record_stream_init(True)
            self._connection_attempt_start_time = None

            error_info = DataSourceErrorInfo(DataSourceErrorKind.ERROR_RESPONSE, error.status, time.time(), str(error))

            http_error_message_result = http_error_message(error.status, "stream connection")
            if not is_http_error_recoverable(error.status):
                log.error(http_error_message_result)
                self._running = False
                self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                await self.__stop_with_error_info(error_info)
                return False
            else:
                log.warning(http_error_message_result)

                if self._data_source_update_sink is not None:
                    self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)
        else:
            log.warning("Unexpected error on stream connection: %s, will retry" % error)
            self._record_stream_init(True)
            self._connection_attempt_start_time = None

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(error)))
            # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of HTTP client internals
        self._connection_attempt_start_time = time.time() + self._sse.next_retry_delay
        return True

    # magic methods for "with" statement (used in testing)
    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        await self.stop()
