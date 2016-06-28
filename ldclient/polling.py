import threading

from ldclient.interfaces import UpdateProcessor
from ldclient.util import log

# TODO account for drift- now we're just pausing 1 second in between requests
class PollingUpdateProcessor(UpdateProcessor):
    def __init__(self, api_key, config, requester, store):
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False
        self._timer = threading.Timer(self._config.poll_interval, self.poll)

    def start(self):
        if not self._running:
            log.debug("Starting PollingUpdateProcessor")
            self._running = True
            self.run()

    def run(self):
        if self._running:
            self._timer = threading.Timer(self._config.poll_interval, self.poll)
            self._timer.start()

    def poll(self):
        self._store.init(self._requester.getAll())
        self.run()

    def initialized(self):
        return self._running and self._store.initialized

    def stop(self):
        log.debug("Closing PollingUpdateProcessor")
        self._running = False
        self._timer.cancel()
