"""
Default implementation of the polling component.
"""
import asyncio
import json
from collections import namedtuple
# currently excluded from documentation - see docs/README.md

from threading import Event

import aiohttp

from ldclient.config import Config
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import UnsuccessfulResponseException, http_error_message, is_http_error_recoverable, log, \
    _headers, throw_if_unsuccessful_response
from ldclient.interfaces import FeatureRequester, AsyncFeatureStore, UpdateProcessor, AsyncDataSourceUpdateSink, \
    DataSourceErrorInfo, DataSourceErrorKind, DataSourceState

import time
from typing import Optional

from ldclient.versioned_data_kind import FEATURES, SEGMENTS

LATEST_ALL_URI = '/sdk/latest-all'

CacheEntry = namedtuple('CacheEntry', ['data', 'etag'])


class AsyncFeatureRequester:
    def __init__(self, config, loop):
        self._cache = dict()
        self._config = config
        self._poll_uri = config.base_uri + LATEST_ALL_URI
        # TODO: Share the same client session with as much of the SDK as possible.
        self._http_client_session = aiohttp.ClientSession(loop=loop)

    async def get_all_data(self):
        uri = self._poll_uri
        headers = _headers(self._config)
        cache_entry = self._cache.get(uri)
        if cache_entry is not None:
            headers['If-None-Match'] = cache_entry.etag

        response = await self._http_client_session.get(uri, headers=headers, timeout=aiohttp.ClientTimeout(
            connect=self._config.http.connect_timeout, sock_read=self._config.http.read_timeout))
        throw_if_unsuccessful_response(response)
        if response.status == 304 and cache_entry is not None:
            data = cache_entry.data
            etag = cache_entry.etag
            from_cache = True
        else:
            data = await response.json(encoding='UTF-8')
            etag = response.headers.get('ETag')
            from_cache = False
            if etag is not None:
                self._cache[uri] = CacheEntry(data=data, etag=etag)
        log.debug("%s response status:[%d] From cache? [%s] ETag:[%s]",
                  uri, response.status, from_cache, etag)

        return {
            FEATURES: data['flags'],
            SEGMENTS: data['segments']
        }


class AsyncPollingUpdateProcessor(UpdateProcessor):
    def __init__(self, config: Config, store: AsyncFeatureStore, ready: asyncio.Event, loop=None):
        self._polling_task = None
        self._config = config
        self._data_source_update_sink: Optional[AsyncDataSourceUpdateSink] = config.data_source_update_sink
        self._store = store
        self._ready = ready
        self._loop = loop
        self._feature_requester = AsyncFeatureRequester(config, loop)

    async def _polling_loop(self):
        while True:
            await self._poll()
            await asyncio.sleep(self._config.poll_interval)

    def start(self):
        log.info("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        if self._polling_task is None:
            self._polling_task = asyncio.run_coroutine_threadsafe(self._polling_loop(), self._loop)

    async def initialized(self):
        return self._ready.is_set() is True and await self._store.initialized is True

    def stop(self):
        self.__stop_with_error_info(None)

    def __stop_with_error_info(self, error: Optional[DataSourceErrorInfo]):
        log.info("Stopping PollingUpdateProcessor")
        self._polling_task.cancel()
        self._polling_task = None

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(
            DataSourceState.OFF,
            error
        )

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

    async def _poll(self):
        try:
            all_data = await self._feature_requester.get_all_data()
            await self._sink_or_store().init(all_data)
            if not self._ready.is_set() and await self._store.initialized:
                log.info("PollingUpdateProcessor initialized ok")
                self._ready.set()

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)
        except UnsuccessfulResponseException as e:
            error_info = DataSourceErrorInfo(
                DataSourceErrorKind.ERROR_RESPONSE,
                e.status,
                time.time(),
                str(e)
            )

            http_error_message_result = http_error_message(e.status, "polling request")
            if not is_http_error_recoverable(e.status):
                log.error(http_error_message_result)
                self._ready.set()  # if client is initializing, make it stop waiting; has no effect if already inited
                self.__stop_with_error_info(error_info)
            else:
                log.warning(http_error_message_result)

                if self._data_source_update_sink is not None:
                    self._data_source_update_sink.update_status(
                        DataSourceState.INTERRUPTED,
                        error_info
                    )
        except Exception as e:
            log.exception(
                'Error: Exception encountered when updating flags. %s' % e)

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(
                    DataSourceState.INTERRUPTED,
                    DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time, str(e))
                )
