"""
Default implementation of the polling component.
"""

# currently excluded from documentation - see docs/README.md

import time
from typing import Optional

from ldclient.async_config import AsyncConfig
from ldclient.impl.aio.concurrency import AsyncEvent, AsyncRepeatingTask
from ldclient.impl.datasource.async_feature_requester import (
    AsyncFeatureRequesterImpl
)
from ldclient.impl.datasource.datasource_common import sink_or_store
from ldclient.impl.util import (
    UnsuccessfulResponseException,
    http_error_message,
    is_http_error_recoverable,
    log
)
from ldclient.interfaces import (
    AsyncFeatureStore,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    UpdateProcessor
)


class AsyncPollingUpdateProcessor(UpdateProcessor):
    def __init__(self, config: AsyncConfig, requester: AsyncFeatureRequesterImpl, store: AsyncFeatureStore, ready: AsyncEvent):
        self._config = config
        self._data_source_update_sink = config.data_source_update_sink
        self._requester = requester
        self._store = store
        self._ready = ready
        self._task = AsyncRepeatingTask("ldclient.datasource.polling", config.poll_interval, 0, self._fetch_and_store)

    def start(self):
        log.info("Starting AsyncPollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        self._task.start()

    def initialized(self):
        return self._ready.is_set() is True and self._store.initialized is True

    async def stop(self):
        self.__stop_with_error_info(None)

    def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
        log.info("Stopping AsyncPollingUpdateProcessor")
        self._task.stop()

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(DataSourceState.OFF, error)

    async def _fetch_and_store(self):
        try:
            all_data = await self._requester.get_all_data()
            if all_data is not None:
                await sink_or_store(self._data_source_update_sink, self._store).init(all_data)
                if not self._ready.is_set() and self._store.initialized:
                    log.info("AsyncPollingUpdateProcessor initialized ok")
                    self._ready.set()

            # Signal VALID on any successful response (200 or 304) once the store is populated.
            if self._store.initialized and self._data_source_update_sink is not None:
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
            if not self._ready.is_set():
                self._ready.set()

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(e)))
