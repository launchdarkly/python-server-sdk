from threading import Thread

from ldclient.interfaces import UpdateProcessor
from ldclient.util import log
import time


class PollingUpdateProcessor(Thread, UpdateProcessor):
    def __init__(self, api_key, config, requester, store):
        Thread.__init__(self)
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False

    def run(self):
        if not self._running:
            log.debug("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
            self._running = True
            while self._running:
                start_time = time.time()
                self._store.init(self._requester.get_all())
                elapsed = time.time() - start_time
                if elapsed < self._config.poll_interval:
                    time.sleep(self._config.poll_interval - elapsed)

    def initialized(self):
        return self._running and self._store.initialized

    def stop(self):
        log.debug("Stopping PollingUpdateProcessor")
        self._running = False
