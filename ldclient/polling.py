from threading import Thread

from ldclient.interfaces import UpdateProcessor
from ldclient.util import log
from ldclient.util import UnsuccessfulResponseException, http_error_message, is_http_error_recoverable

import time


class PollingUpdateProcessor(Thread, UpdateProcessor):
    def __init__(self, config, requester, store, ready):
        Thread.__init__(self)
        self.daemon = True
        self._config = config
        self._requester = requester
        self._store = store
        self._running = False
        self._ready = ready

    def run(self):
        if not self._running:
            log.info("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
            self._running = True
            while self._running:
                start_time = time.time()
                try:
                    all_data = self._requester.get_all_data()
                    self._store.init(all_data)
                    if not self._ready.is_set() is True and self._store.initialized is True:
                        log.info("PollingUpdateProcessor initialized ok")
                        self._ready.set()
                except UnsuccessfulResponseException as e:
                    log.error(http_error_message(e.status, "polling request"))
                    if not is_http_error_recoverable(e.status):
                        self._ready.set() # if client is initializing, make it stop waiting; has no effect if already inited
                        self.stop()
                except Exception as e:
                    log.exception(
                        'Error: Exception encountered when updating flags. %s' % e)

                elapsed = time.time() - start_time
                if elapsed < self._config.poll_interval:
                    time.sleep(self._config.poll_interval - elapsed)

    def initialized(self):
        return self._running and self._ready.is_set() is True and self._store.initialized is True

    def stop(self):
        log.info("Stopping PollingUpdateProcessor")
        self._running = False
