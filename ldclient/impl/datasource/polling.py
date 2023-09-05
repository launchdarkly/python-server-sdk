"""
Default implementation of the polling component.
"""
# currently excluded from documentation - see docs/README.md

from threading import Event

from ldclient.config import Config
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import UnsuccessfulResponseException, http_error_message, is_http_error_recoverable, log
from ldclient.interfaces import FeatureRequester, FeatureStore, UpdateProcessor


class PollingUpdateProcessor(UpdateProcessor):
    def __init__(self, config: Config, requester: FeatureRequester, store: FeatureStore, ready: Event):
        self._config = config
        self._requester = requester
        self._store = store
        self._ready = ready
        self._task = RepeatingTask(config.poll_interval, 0, self._poll)

    def start(self):
        log.info("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        self._task.start()

    def initialized(self):
        return self._ready.is_set() is True and self._store.initialized is True

    def stop(self):
        log.info("Stopping PollingUpdateProcessor")
        self._task.stop()

    def _poll(self):
        try:
            all_data = self._requester.get_all_data()
            self._store.init(all_data)
            if not self._ready.is_set() and self._store.initialized:
                log.info("PollingUpdateProcessor initialized ok")
                self._ready.set()
        except UnsuccessfulResponseException as e:
            http_error_message_result = http_error_message(e.status, "polling request")
            if is_http_error_recoverable(e.status):
                log.warning(http_error_message_result)
            else:
                log.error(http_error_message_result)
                self._ready.set() # if client is initializing, make it stop waiting; has no effect if already inited
                self.stop()
        except Exception as e:
            log.exception(
                'Error: Exception encountered when updating flags. %s' % e)
