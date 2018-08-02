from __future__ import absolute_import
from collections import namedtuple

import json
from threading import Thread

import backoff
import logging
import time

from ldclient.interfaces import UpdateProcessor
from ldclient.sse_client import SSEClient
from ldclient.util import _stream_headers, log, UnsuccessfulResponseException, http_error_message, is_http_error_recoverable
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# allows for up to 5 minutes to elapse without any data sent across the stream. The heartbeats sent as comments on the
# stream will keep this from triggering
stream_read_timeout = 5 * 60

STREAM_ALL_PATH = '/all'

ParsedPath = namedtuple('ParsedPath', ['kind', 'key'])


class StreamingUpdateProcessor(Thread, UpdateProcessor):
    def __init__(self, config, requester, store, ready):
        Thread.__init__(self)
        self.daemon = True
        self._uri = config.stream_base_uri + STREAM_ALL_PATH
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False
        self._ready = ready

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
        while self._running:
            try:
                messages = self._connect()
                for msg in messages:
                    if not self._running:
                        break
                    message_ok = self.process_message(self._store, self._requester, msg)
                    if message_ok is True and self._ready.is_set() is False:
                        log.info("StreamingUpdateProcessor initialized ok.")
                        self._ready.set()
            except UnsuccessfulResponseException as e:
                log.error(http_error_message(e.status, "stream connection"))
                if not is_http_error_recoverable(e.status):
                    self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                    self.stop()
                    break
            except Exception as e:
                log.warning("Caught exception. Restarting stream connection after one second. %s" % e)
                # no stacktrace here because, for a typical connection error, it'll just be a lengthy tour of urllib3 internals
            time.sleep(1)

    def _backoff_expo():
        return backoff.expo(max_value=30)

    def should_not_retry(e):
        return isinstance(e, UnsuccessfulResponseException) and (not is_http_error_recoverable(e.status))

    def log_backoff_message(props):
        log.error("Streaming connection failed, will attempt to restart")
        log.info("Will reconnect after delay of %fs", props['wait'])

    @backoff.on_exception(_backoff_expo, BaseException, max_tries=None, jitter=backoff.full_jitter,
                          on_backoff=log_backoff_message, giveup=should_not_retry)
    def _connect(self):
        return SSEClient(
            self._uri,
            headers=_stream_headers(self._config.sdk_key),
            connect_timeout=self._config.connect_timeout,
            read_timeout=stream_read_timeout,
            verify_ssl=self._config.verify_ssl)

    def stop(self):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    # Returns True if we initialized the feature store
    @staticmethod
    def process_message(store, requester, msg):
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
        elif msg.event == "indirect/patch":
            path = msg.data
            log.debug("Received indirect/patch event for %s", path)
            target = StreamingUpdateProcessor._parse_path(path)
            if target is not None:
                store.upsert(target.kind, requester.get_one(target.kind, target.key))
            else:
                log.warning("Indirect patch for unknown path: %s", path)
        elif msg.event == "indirect/put":
            log.debug("Received indirect/put event")
            store.init(requester.get_all_data())
            return True
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
