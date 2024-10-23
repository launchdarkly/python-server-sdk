"""
Default implementation of the polling component.
"""

# currently excluded from documentation - see docs/README.md

import time
from threading import Event
from typing import Optional

from ldclient.config import Config
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import (UnsuccessfulResponseException,
                                http_error_message, is_http_error_recoverable,
                                log)
from ldclient.interfaces import (DataSourceErrorInfo, DataSourceErrorKind,
                                 DataSourceState, DataSourceUpdateSink,
                                 FeatureRequester, FeatureStore,
                                 UpdateProcessor)


class PollingUpdateProcessor(UpdateProcessor):
    def __init__(self, config: Config, requester: FeatureRequester, store: FeatureStore, ready: Event):
        self._config = config
        self._data_source_update_sink: Optional[DataSourceUpdateSink] = config.data_source_update_sink
        self._requester = requester
        self._store = store
        self._ready = ready
        self._task = RepeatingTask("ldclient.datasource.polling", config.poll_interval, 0, self._poll)

    def start(self):
        log.info("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        self._task.start()

    def initialized(self):
        return self._ready.is_set() is True and self._store.initialized is True

    def stop(self):
        self.__stop_with_error_info(None)

    def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
        log.info("Stopping PollingUpdateProcessor")
        self._task.stop()

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(DataSourceState.OFF, error)

    def _sink_or_store(self):
        """
        The original implementation of this class relied on the feature store
        directly, which we are trying to move away from. Customers who might have
        instantiated this directly for some reason wouldn't know they have to set
        the config's sink manually, so we have to fall back to the store if the
        sink isn't present.

        The next major release should be able to simplify this structure and
        remove the need for fall back to the data store because the update sink
        should always be present.
        """
        if self._data_source_update_sink is None:
            return self._store

        return self._data_source_update_sink

    def _poll(self):
        try:
            all_data = self._requester.get_all_data()
            self._sink_or_store().init(all_data)
            if not self._ready.is_set() and self._store.initialized:
                log.info("PollingUpdateProcessor initialized ok")
                self._ready.set()

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)
        except UnsuccessfulResponseException as e:
            error_info = DataSourceErrorInfo(DataSourceErrorKind.ERROR_RESPONSE, e.status, time.time(), str(e))

            http_error_message_result = http_error_message(e.status, "polling request")
            if not is_http_error_recoverable(e.status):
                log.error(http_error_message_result)
                self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                self.__stop_with_error_info(error_info)
            else:
                log.warning(http_error_message_result)

                if self._data_source_update_sink is not None:
                    self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)
        except Exception as e:
            log.exception('Error: Exception encountered when updating flags. %s' % e)

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time, str(e)))
