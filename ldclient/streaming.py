import json
from threading import Thread

from sseclient import SSEClient

from ldclient.interfaces import UpdateProcessor
from ldclient.util import _stream_headers, log


class StreamingUpdateProcessor(Thread, UpdateProcessor):

    def __init__(self, api_key, config, requester, store):
        Thread.__init__(self)
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False

    def run(self):
        log.debug("Starting StreamingUpdateProcessor")
        self._running = True
        hdrs = _stream_headers(self._api_key)
        uri = self._config.stream_features_uri
        messages = SSEClient(uri, verify=self._config.verify, headers=hdrs)
        for msg in messages:
            if not self._running:
                break
            self.process_message(self._store, self._requester, msg)

    def stop(self):
        self._running = False

    def initialized(self):
        return self._running

    @staticmethod
    def process_message(store, requester, msg):
        payload = json.loads(msg.data)
        log.debug("Received stream event {}".format(msg.event))
        if msg.event == 'put':
            store.init(payload)
        elif msg.event == 'patch':
            key = payload['path'][1:]
            feature = payload['data']
            log.debug("Updating feature {}".format(key))
            store.upsert(key, feature)
        elif msg.event == "indirect/patch":
            key = payload['data']
            store.upsert(key, requester.get(key))
        elif msg.event == "indirect/put":
            store.init(requester.getAll())
        elif msg.event == 'delete':
            key = payload['path'][1:]
            # noinspection PyShadowingNames
            version = payload['version']
            store.delete(key, version)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)