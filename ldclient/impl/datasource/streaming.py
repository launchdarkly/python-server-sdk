import json
import time
from threading import TIMEOUT_MAX
from threading import Event as ThreadEvent
from threading import Thread
from typing import Callable, Optional
from urllib import parse

from ld_eventsource import SSEClient
from ld_eventsource.actions import Event, Fault, Start
from ld_eventsource.config import (
    ConnectStrategy,
    ErrorStrategy,
    RetryDelayStrategy
)
from ld_eventsource.errors import HTTPStatusError

from ldclient.impl.datasource.datasource_common import (
    STREAM_ALL_PATH,
    StreamClosedError,
    parse_path,
    record_environment_id,
    sink_or_store
)
from ldclient.impl.http import HTTPFactory, _http_factory
from ldclient.impl.retry import (
    FailureKind,
    RetryState,
    classify_http_status,
    for_streaming
)
from ldclient.impl.util import http_error_description, log
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    UpdateProcessor
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# allows for up to 5 minutes to elapse without any data sent across the stream. The heartbeats sent as comments on the
# stream will keep this from triggering
stream_read_timeout = 5 * 60


class StreamingUpdateProcessor(Thread, UpdateProcessor):
    """Reads flag data from LaunchDarkly's streaming endpoint on its own thread.

    The SDK owns the delay between connection attempts rather than the SSE
    client; see :meth:`_create_sse_client` and :mod:`ldclient.impl.retry`.
    """

    def __init__(self, config, store, ready, diagnostic_accumulator, retry_state: Optional[RetryState] = None):
        Thread.__init__(self, name="ldclient.datasource.streaming")
        self.daemon = True
        self._uri = config.stream_base_uri + STREAM_ALL_PATH
        if config.payload_filter_key is not None:
            self._uri += '?%s' % parse.urlencode({'filter': config.payload_filter_key})
        self._config = config
        self._data_source_update_sink = config.data_source_update_sink
        self._store = store
        self._running = False
        self._ready = ready
        self._diagnostic_accumulator = diagnostic_accumulator
        self._connection_attempt_start_time: Optional[float] = None
        self._retry = retry_state or for_streaming(config.initial_reconnect_delay)
        self._stop_event = ThreadEvent()
        self._interrupted_by_sdk = False

    def run(self):
        log.info("Starting StreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        self._sse = self._create_sse_client()
        self._connection_attempt_start_time = time.time()
        for action in self._sse.all:
            if isinstance(action, Start):
                # interrupt() is a no-op when the connection has already gone, so
                # clear a stale flag here rather than swallow the next real close.
                self._interrupted_by_sdk = False
                record_environment_id(self._data_source_update_sink, action.headers)
            elif isinstance(action, Event):
                message_ok = False
                message_handled = False
                try:
                    message_ok = self._process_message(sink_or_store(self._data_source_update_sink, self._store), action)
                    message_handled = True
                except json.decoder.JSONDecodeError as e:
                    log.info("Error while handling stream event; will restart stream: %s" % e)
                    self._interrupt_stream()

                    if not self._handle_error(e):
                        break
                except Exception as e:
                    log.info("Error while handling stream event; will restart stream: %s" % e)
                    self._interrupt_stream()

                    if not self._handle_error(e):
                        break

                if message_handled:
                    self._retry.record_success()

                if message_ok:
                    self._record_stream_init(False)
                    self._connection_attempt_start_time = None

                    if self._data_source_update_sink is not None:
                        self._data_source_update_sink.update_status(DataSourceState.VALID, None)

                    if not self._ready.is_set():
                        log.info("StreamingUpdateProcessor initialized ok.")
                        self._ready.set()
            elif isinstance(action, Fault):
                # A Fault with no error is a clean close. An interrupt the SDK
                # asked for is not a failure.
                if action.error is None:
                    if self._interrupted_by_sdk:
                        self._interrupted_by_sdk = False
                        continue
                    if not self._handle_error(StreamClosedError()):
                        break
                    continue

                if not self._handle_error(action.error):
                    break
        self._sse.close()

    def _record_stream_init(self, failed: bool):
        if self._diagnostic_accumulator and self._connection_attempt_start_time:
            current_time = int(time.time() * 1000)
            elapsed = current_time - int(self._connection_attempt_start_time * 1000)
            self._diagnostic_accumulator.record_stream_init(current_time, elapsed if elapsed >= 0 else 0, failed)

    def _create_sse_client(self) -> SSEClient:
        # We don't want the stream to use the same read timeout as the rest of the SDK.
        http_factory = _http_factory(self._config)
        stream_http_factory = HTTPFactory(http_factory.base_headers, http_factory.http_config, override_read_timeout=stream_read_timeout)
        return SSEClient(
            connect=ConnectStrategy.http(
                url=self._uri, headers=http_factory.base_headers, pool=stream_http_factory.create_pool_manager(1, self._uri), urllib3_request_options={"timeout": stream_http_factory.timeout}
            ),
            error_strategy=ErrorStrategy.always_continue(),  # we'll make error-handling decisions when we see a Fault
            # The SSE client's retry is disabled; the SDK owns the delay. The base
            # strategy must be passed: omitting it selects the library's backoff.
            initial_retry_delay=0,
            retry_delay_strategy=RetryDelayStrategy(),
            retry_delay_reset_threshold=0,
            logger=log,
        )

    def stop(self):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False
        self._stop_event.set()
        if self._sse:
            self._sse.close()

        if self._data_source_update_sink is None:
            return

        # OFF means an explicit shutdown. No stream failure produces it.
        self._data_source_update_sink.update_status(DataSourceState.OFF, None)

    def _interrupt_stream(self):
        """Drops the stream connection so the next read reconnects. The SSE
        client reports the close as a Fault with no error, and the flag tells
        the loop that this one is ours and is already accounted for."""
        self._interrupted_by_sdk = True
        self._sse.interrupt()

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    # Returns True if we initialized the feature store
    def _process_message(self, store, msg: Event) -> bool:
        if msg.event == 'put':
            all_data = json.loads(msg.data)
            init_data = {FEATURES: all_data['data']['flags'], SEGMENTS: all_data['data']['segments']}
            log.debug("Received put event with %d flags and %d segments", len(init_data[FEATURES]), len(init_data[SEGMENTS]))
            store.init(init_data)
            return True
        elif msg.event == 'patch':
            payload = json.loads(msg.data)
            path = payload['path']
            obj = payload['data']
            log.debug("Received patch event for %s, New version: [%d]", path, obj.get("version"))
            target = parse_path(path)
            if target is not None:
                store.upsert(target.kind, obj)
            else:
                log.warning("Patch for unknown path")
        elif msg.event == 'delete':
            payload = json.loads(msg.data)
            path = payload['path']
            # noinspection PyShadowingNames
            version = payload['version']
            log.debug("Received delete event for %s, New version: [%d]", path, version)
            target = parse_path(path)
            if target is not None:
                store.delete(target.kind, target.key, version)
            else:
                log.warning("Delete for unknown path")
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)
        return False

    # Returns true to continue, false to stop
    def _handle_error(self, error: Exception) -> bool:
        """Records a stream failure, reports it, and waits before the retry.

        Returns True once the wait is over, or False if the processor was
        stopped. No failure ever ends the stream by itself.
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
            kind = FailureKind.NORMAL
            error_info = DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(error))
            # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of urllib3 internals
            description = "Error on stream connection: %s" % error
            level = log.warning

        self._retry.record_failure(kind)
        delay = self._retry.next_delay
        level("%s - will retry in %.1fs" % (description, delay))

        if self._data_source_update_sink is not None:
            self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)

        interrupted = self._stop_event.wait(min(delay, TIMEOUT_MAX))

        # Read after the wait, so a clock change during it cannot skew the
        # stream-init latency we report.
        self._connection_attempt_start_time = time.time()
        return not interrupted

    # magic methods for "with" statement (used in testing)
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()
