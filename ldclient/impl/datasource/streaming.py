import json
import time
from collections import namedtuple
from threading import Thread
from typing import Optional
from urllib import parse

from ld_eventsource import SSEClient
from ld_eventsource.actions import Event, Fault
from ld_eventsource.config import (ConnectStrategy, ErrorStrategy,
                                   RetryDelayStrategy)
from ld_eventsource.errors import HTTPStatusError

from ldclient.impl.http import HTTPFactory, _http_factory
from ldclient.impl.util import (http_error_message, is_http_error_recoverable,
                                log)
from ldclient.interfaces import (DataSourceErrorInfo, DataSourceErrorKind,
                                 DataSourceState, UpdateProcessor)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# allows for up to 5 minutes to elapse without any data sent across the stream. The heartbeats sent as comments on the
# stream will keep this from triggering
stream_read_timeout = 5 * 60

MAX_RETRY_DELAY = 30
BACKOFF_RESET_INTERVAL = 60
JITTER_RATIO = 0.5

STREAM_ALL_PATH = '/all'

ParsedPath = namedtuple('ParsedPath', ['kind', 'key'])


class StreamingUpdateProcessor(Thread, UpdateProcessor):
    def __init__(self, config, store, ready, diagnostic_accumulator):
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
        self._connection_attempt_start_time = None

    def run(self):
        log.info("Starting StreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        self._sse = self._create_sse_client()
        self._connection_attempt_start_time = time.time()
        for action in self._sse.all:
            if isinstance(action, Event):
                message_ok = False
                try:
                    message_ok = self._process_message(self._sink_or_store(), action)
                except json.decoder.JSONDecodeError as e:
                    log.info("Error while handling stream event; will restart stream: %s" % e)
                    self._sse.interrupt()

                    self._handle_error(e)
                except Exception as e:
                    log.info("Error while handling stream event; will restart stream: %s" % e)
                    self._sse.interrupt()

                    if self._data_source_update_sink is not None:
                        error_info = DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(e))

                        self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)

                if message_ok:
                    self._record_stream_init(False)
                    self._connection_attempt_start_time = None

                    if self._data_source_update_sink is not None:
                        self._data_source_update_sink.update_status(DataSourceState.VALID, None)

                    if not self._ready.is_set():
                        log.info("StreamingUpdateProcessor initialized ok.")
                        self._ready.set()
            elif isinstance(action, Fault):
                # If the SSE client detects the stream has closed, then it will emit a fault with no-error. We can
                # ignore this since we want the connection to continue.
                if action.error is None:
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
            initial_retry_delay=self._config.initial_reconnect_delay,
            retry_delay_strategy=RetryDelayStrategy.default(max_delay=MAX_RETRY_DELAY, backoff_multiplier=2, jitter_multiplier=JITTER_RATIO),
            retry_delay_reset_threshold=BACKOFF_RESET_INTERVAL,
            logger=log,
        )

    def stop(self):
        self.__stop_with_error_info(None)

    def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False
        if self._sse:
            self._sse.close()

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(DataSourceState.OFF, error)

    def _sink_or_store(self):
        if self._data_source_update_sink is None:
            return self._store

        return self._data_source_update_sink

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
            target = StreamingUpdateProcessor._parse_path(path)
            if target is not None:
                store.upsert(target.kind, obj)
            else:
                log.warning("Patch for unknown path: %s", path)
        elif msg.event == 'delete':
            payload = json.loads(msg.data)
            path = payload['path']
            # noinspection PyShadowingNames
            version = payload['version']
            log.debug("Received delete event for %s, New version: [%d]", path, version)
            target = StreamingUpdateProcessor._parse_path(path)
            if target is not None:
                store.delete(target.kind, target.key, version)
            else:
                log.warning("Delete for unknown path: %s", path)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)
        return False

    # Returns true to continue, false to stop
    def _handle_error(self, error: Exception) -> bool:
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
                self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                self.__stop_with_error_info(error_info)
                self.stop()
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
            # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of urllib3 internals
        self._connection_attempt_start_time = time.time() + self._sse.next_retry_delay
        return True

    @staticmethod
    def _parse_path(path: str):
        for kind in [FEATURES, SEGMENTS]:
            if path.startswith(kind.stream_api_path):
                return ParsedPath(kind=kind, key=path[len(kind.stream_api_path):])
        return None

    # magic methods for "with" statement (used in testing)
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()
