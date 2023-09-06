"""
Default implementation of the streaming component.
"""
# currently excluded from documentation - see docs/README.md

from collections import namedtuple

import json
from threading import Thread

import logging
import time

from ldclient.impl.http import HTTPFactory, _http_factory
from ldclient.impl.retry_delay import RetryDelayStrategy, DefaultBackoffStrategy, DefaultJitterStrategy
from ldclient.impl.sse import SSEClient
from ldclient.impl.util import log, UnsuccessfulResponseException, http_error_message, is_http_error_recoverable
from ldclient.interfaces import UpdateProcessor
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
        Thread.__init__(self)
        self.daemon = True
        self._uri = config.stream_base_uri + STREAM_ALL_PATH
        self._config = config
        self._store = store
        self._running = False
        self._ready = ready
        self._diagnostic_accumulator = diagnostic_accumulator
        self._es_started = None
        self._retry_delay = RetryDelayStrategy(
            config.initial_reconnect_delay,
            BACKOFF_RESET_INTERVAL,
            DefaultBackoffStrategy(MAX_RETRY_DELAY),
            DefaultJitterStrategy(JITTER_RATIO))

        # We need to suppress the default logging behavior of the backoff package, because
        # it logs messages at ERROR level with variable content (the delay time) which will
        # prevent monitors from coalescing multiple messages. The backoff package attempts
        # to suppress its own output by default by giving the logger a NullHandler, but it
        # will still propagate up to the root logger unless we do this:
        logging.getLogger('backoff').propagate = False

    # Retry/backoff logic:
    # Upon any error establishing the stream connection we retry with backoff + jitter.
    # Upon any error processing the results of the stream we reconnect after one second.
    def run(self):
        log.info("Starting StreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        attempts = 0
        while self._running:
            if attempts > 0:
                delay = self._retry_delay.next_retry_delay(time.time())
                log.info("Will reconnect after delay of %fs" % delay)
                time.sleep(delay)
            attempts += 1
            try:
                self._es_started = int(time.time() * 1000)
                messages = self._connect()
                for msg in messages:
                    if not self._running:
                        break
                    self._retry_delay.set_good_since(time.time())
                    message_ok = self.process_message(self._store, msg)
                    if message_ok:
                        self._record_stream_init(False)
                        self._es_started = None
                    if message_ok is True and self._ready.is_set() is False:
                        log.info("StreamingUpdateProcessor initialized ok.")
                        self._ready.set()
            except UnsuccessfulResponseException as e:
                self._record_stream_init(True)
                self._es_started = None

                http_error_message_result = http_error_message(e.status, "stream connection")
                if is_http_error_recoverable(e.status):
                    log.warning(http_error_message_result)
                else:
                    log.error(http_error_message_result)
                    self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                    self.stop()
                    break
            except Exception as e:
                log.warning("Unexpected error on stream connection: %s, will retry" % e)
                self._record_stream_init(True)
                self._es_started = None
                # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of urllib3 internals

    def _record_stream_init(self, failed):
        if self._diagnostic_accumulator and self._es_started:
            current_time = int(time.time() * 1000)
            self._diagnostic_accumulator.record_stream_init(current_time, current_time - self._es_started, failed)

    def _connect(self):
        # We don't want the stream to use the same read timeout as the rest of the SDK.
        http_factory = _http_factory(self._config)
        stream_http_factory = HTTPFactory(http_factory.base_headers, http_factory.http_config, override_read_timeout=stream_read_timeout)
        client = SSEClient(
            self._uri,
            http_factory = stream_http_factory
        )
        return client.events

    def stop(self):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    # Returns True if we initialized the feature store
    @staticmethod
    def process_message(store, msg):
        if msg.event == 'put':
            all_data = json.loads(msg.data)
            init_data = {
                FEATURES: all_data['data']['flags'],
                SEGMENTS: all_data['data']['segments']
            }
            log.debug("Received put event with %d flags and %d segments",
                len(init_data[FEATURES]), len(init_data[SEGMENTS]))
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

    @staticmethod
    def _parse_path(path):
        for kind in [FEATURES, SEGMENTS]:
            if path.startswith(kind.stream_api_path):
                return ParsedPath(kind = kind, key = path[len(kind.stream_api_path):])
        return None

    # magic methods for "with" statement (used in testing)
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.stop()
