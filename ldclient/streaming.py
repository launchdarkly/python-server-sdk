from __future__ import absolute_import

import json
from threading import Thread

import backoff
import requests
from ldclient.interfaces import UpdateProcessor
from ldclient.sse_client import SSEClient
from ldclient.util import _stream_headers, log


class StreamingUpdateProcessor(Thread, UpdateProcessor):
    def __init__(self, config, requester, store, ready):
        Thread.__init__(self)
        self.daemon = True
        self._uri = config.stream_uri
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False
        self._ready = ready

    def run(self):
        log.info("Starting StreamingUpdateProcessor connecting to uri: " + self._uri)
        self._running = True
        while self._running:
            self._connect()

    def _backoff_expo():
        return backoff.expo(max_value=30)

    @backoff.on_exception(_backoff_expo, requests.exceptions.RequestException, max_tries=None, jitter=backoff.full_jitter)
    def _connect(self):
        messages = SSEClient(self._uri, verify=self._config.verify_ssl, headers=_stream_headers(self._config.sdk_key))
        for msg in messages:
            if not self._running:
                break
            message_ok = self.process_message(self._store, self._requester, msg, self._ready)
            if message_ok is True and self._ready.is_set() is False:
                self._ready.set()

    def stop(self):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    @staticmethod
    def process_message(store, requester, msg, ready):
        log.debug("Received stream event {} with data: {}".format(msg.event, msg.data))
        if msg.event == 'put':
            payload = json.loads(msg.data)
            store.init(payload)
            if not ready.is_set() is True and store.initialized is True:
                log.info("StreamingUpdateProcessor initialized ok")
                return True
        elif msg.event == 'patch':
            payload = json.loads(msg.data)
            key = payload['path'][1:]
            feature = payload['data']
            store.upsert(key, feature)
        elif msg.event == "indirect/patch":
            key = msg.data
            store.upsert(key, requester.get_one(key))
        elif msg.event == "indirect/put":
            store.init(requester.get_all())
            if not ready.is_set() is True and store.initialized is True:
                log.info("StreamingUpdateProcessor initialized ok")
                return True
        elif msg.event == 'delete':
            payload = json.loads(msg.data)
            key = payload['path'][1:]
            # noinspection PyShadowingNames
            version = payload['version']
            store.delete(key, version)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)
        return False
