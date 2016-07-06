import json
from threading import Thread

from sseclient import SSEClient

from ldclient.interfaces import UpdateProcessor
from ldclient.util import _stream_headers, log


class StreamingUpdateProcessor(Thread, UpdateProcessor):

    def __init__(self, api_key, config, requester, store, ready):
        Thread.__init__(self)
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False
        self._ready = ready

    def run(self):
        log.info("Starting StreamingUpdateProcessor connecting to uri: " + self._config.stream_uri)
        self._running = True
        hdrs = _stream_headers(self._api_key)
        uri = self._config.stream_uri
        messages = SSEClient(uri, verify=self._config.verify_ssl, headers=hdrs)
        for msg in messages:
            if not self._running:
                break
            self.process_message(self._store, self._requester, msg, self._ready)

    def stop(self):
        log.info("Stopping StreamingUpdateProcessor")
        self._running = False

    def initialized(self):
        return self._running and self._ready.is_set() and self._store.initialized

    @staticmethod
    def process_message(store, requester, msg, ready):
        payload = json.loads(msg.data)
        log.debug("Received stream event {}".format(msg.event))
        if msg.event == 'put':
            store.init(payload)
            if not ready.is_set() and store.initialized:
                ready.set()
                log.info("StreamingUpdateProcessor initialized ok")
        elif msg.event == 'patch':
            key = payload['path'][1:]
            feature = payload['data']
            log.debug("Updating feature {}".format(key))
            store.upsert(key, feature)
        elif msg.event == "indirect/patch":
            key = payload['data']
            store.upsert(key, requester.get_one(key))
        elif msg.event == "indirect/put":
            store.init(requester.get_all())
            if not ready.is_set() and store.initialized:
                ready.set()
                log.info("StreamingUpdateProcessor initialized ok")
        elif msg.event == 'delete':
            key = payload['path'][1:]
            # noinspection PyShadowingNames
            version = payload['version']
            store.delete(key, version)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)