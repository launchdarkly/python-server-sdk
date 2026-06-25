"""
Tests for AsyncFeatureRequester and AsyncPollingUpdateProcessor.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ldclient.config import Config
from ldclient.impl.aio.transport_types import TransportResponse
from ldclient.impl.datasource.async_feature_requester import (
    AsyncFeatureRequester
)
from ldclient.impl.datasource.async_polling import AsyncPollingUpdateProcessor
from ldclient.impl.util import UnsuccessfulResponseException
from ldclient.interfaces import DataSourceErrorKind, DataSourceState
from ldclient.testing.mock_async_components import MockAsyncFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# Sample data returned by a successful poll
SAMPLE_FLAGS = {'flagkey': {'key': 'flagkey', 'version': 1, 'deleted': False}}
SAMPLE_SEGMENTS = {'segkey': {'key': 'segkey', 'version': 1, 'deleted': False}}
SAMPLE_DATA = {FEATURES: SAMPLE_FLAGS, SEGMENTS: SAMPLE_SEGMENTS}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_config(**kwargs):
    """Create a Config with a short poll_interval for tests.

    Config enforces a minimum poll_interval of 30s, so we patch the property.
    """
    return Config('SDK_KEY', **kwargs)


def make_processor(config=None, store=None, ready=None, requester=None):
    if config is None:
        config = make_config()
    if store is None:
        store = MockAsyncFeatureStore()
    if ready is None:
        ready = asyncio.Event()
    if requester is None:
        requester = MagicMock()
    return AsyncPollingUpdateProcessor(
        config=config,
        requester=requester,
        store=store,
        ready=ready,
    )


# ---------------------------------------------------------------------------
# AsyncFeatureRequester tests
# ---------------------------------------------------------------------------

def make_transport(*responses: TransportResponse):
    """Create a stub transport whose request() returns the given responses in order."""
    transport = MagicMock()
    transport.request = AsyncMock(side_effect=list(responses))
    return transport


class TestAsyncFeatureRequester:
    @pytest.mark.asyncio
    async def test_successful_response_returns_flags_and_segments(self):
        import json
        config = make_config()
        transport = make_transport(
            TransportResponse(200, {}, json.dumps({'flags': SAMPLE_FLAGS, 'segments': SAMPLE_SEGMENTS}))
        )
        requester = AsyncFeatureRequester(config, transport)

        data = await requester.get_all_data()

        assert data[FEATURES] == SAMPLE_FLAGS
        assert data[SEGMENTS] == SAMPLE_SEGMENTS

    @pytest.mark.asyncio
    async def test_304_not_modified_returns_cached_data(self):
        from ldclient.impl.datasource.async_feature_requester import CacheEntry

        config = make_config()
        transport = make_transport(TransportResponse(304, {}, ''))
        requester = AsyncFeatureRequester(config, transport)

        # Pre-populate the cache with a known etag and data
        cached_data = {'flags': SAMPLE_FLAGS, 'segments': SAMPLE_SEGMENTS}
        requester._cache[requester._poll_uri] = CacheEntry(data=cached_data, etag='"abc"')

        data = await requester.get_all_data()

        # 304 returns the cached data rather than None
        assert data[FEATURES] == SAMPLE_FLAGS
        assert data[SEGMENTS] == SAMPLE_SEGMENTS
        # The cached etag is sent as If-None-Match
        headers = transport.request.call_args.kwargs['headers']
        assert headers['If-None-Match'] == '"abc"'

    @pytest.mark.asyncio
    async def test_etag_and_data_stored_after_successful_response(self):
        config = make_config()
        transport = make_transport(
            TransportResponse(200, {'ETag': '"v1"'}, '{"flags": {}, "segments": {}}')
        )
        requester = AsyncFeatureRequester(config, transport)

        await requester.get_all_data()

        cache_entry = requester._cache.get(requester._poll_uri)
        assert cache_entry is not None
        assert cache_entry.etag == '"v1"'

    @pytest.mark.asyncio
    async def test_http_error_raises_unsuccessful_response_exception(self):
        config = make_config()
        transport = make_transport(TransportResponse(401, {}, ''))
        requester = AsyncFeatureRequester(config, transport)

        with pytest.raises(UnsuccessfulResponseException) as exc_info:
            await requester.get_all_data()

        assert exc_info.value.status == 401

    @pytest.mark.asyncio
    async def test_payload_filter_key_appended_to_uri(self):
        config = Config('SDK_KEY', payload_filter_key='my-filter')
        requester = AsyncFeatureRequester(config, MagicMock())

        assert 'filter=my-filter' in requester._poll_uri


# ---------------------------------------------------------------------------
# AsyncPollingUpdateProcessor tests
# ---------------------------------------------------------------------------

class TestAsyncPollingUpdateProcessor:

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_successful_poll_initializes_store_and_sets_ready(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        assert ready.is_set()
        assert store.initialized
        assert processor.initialized()
        assert len(store.inits) >= 1
        assert store.inits[0] == SAMPLE_DATA

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_304_not_modified_does_not_reinitialize_store(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        call_count = 0

        async def get_all_data():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return SAMPLE_DATA
            return None  # Simulate 304 Not Modified on subsequent calls

        processor._requester.get_all_data = get_all_data

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        # Let it run at least a second poll cycle
        await asyncio.sleep(0.05)

        # Store should only have been initialized once (None return does not trigger re-init)
        assert len(store.inits) == 1

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_unrecoverable_http_error_stops_polling_and_sets_ready(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        mock_requester = AsyncMock(side_effect=UnsuccessfulResponseException(401))
        processor._requester.get_all_data = mock_requester

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        assert ready.is_set()
        assert not processor.initialized()

        # The polling task must have stopped itself: no further polls occur.
        await asyncio.sleep(0.05)
        snapshot = mock_requester.call_count
        await asyncio.sleep(0.05)
        assert mock_requester.call_count == snapshot

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_recoverable_http_error_continues_polling(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        call_count = 0

        async def get_all_data():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise UnsuccessfulResponseException(500)
            return SAMPLE_DATA

        processor._requester.get_all_data = get_all_data

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        assert ready.is_set()
        assert processor.initialized()
        assert call_count >= 3

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_general_exception_does_not_stop_polling(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        call_count = 0

        async def get_all_data():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient error")
            return SAMPLE_DATA

        processor._requester.get_all_data = get_all_data

        processor.start()

        # _ready is set on the first exception (so the client is never stuck),
        # but the loop continues.  Wait until the store is actually initialized
        # (call_count reaches 3) to verify polling kept running.
        async def wait_for_initialized():
            while not store.initialized:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_initialized(), timeout=2.0)

        assert ready.is_set()
        assert call_count >= 3

        await processor.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_polling_task_cleanly(self):
        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        poll_count = 0

        async def slow_poll():
            nonlocal poll_count
            poll_count += 1
            await asyncio.sleep(60)  # Would block indefinitely without cancel

        processor._requester.get_all_data = slow_poll

        processor.start()
        await asyncio.sleep(0.05)  # Let the task start

        # stop() should return promptly even though the poll is "sleeping"
        await asyncio.wait_for(processor.stop(), timeout=1.0)

        # No further polls occur after stopping
        await asyncio.sleep(0.05)
        assert poll_count == 1

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_unrecoverable_error_updates_sink_to_off(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()

        sink = AsyncMock()
        config._data_source_update_sink = sink

        processor = make_processor(config=config, store=store, ready=ready)
        processor._data_source_update_sink = sink

        processor._requester.get_all_data = AsyncMock(
            side_effect=UnsuccessfulResponseException(403)
        )

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        # Verify the sink was told to go OFF
        calls = [call for call in sink.update_status.call_args_list if call.args[0] == DataSourceState.OFF]
        assert len(calls) >= 1
        error_info = calls[0].args[1]
        assert error_info.kind == DataSourceErrorKind.ERROR_RESPONSE
        assert error_info.status_code == 403

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_successful_poll_updates_sink_to_valid(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()

        # The processor checks self._store.initialized before sending VALID.
        # Use an AsyncMock sink whose init() also marks the underlying store as initialized
        # so that both the sink-path and the initialized check work correctly.
        sink = AsyncMock()

        async def _init_and_store(data):
            await store.init(data)

        sink.init = _init_and_store
        config._data_source_update_sink = sink

        processor = make_processor(config=config, store=store, ready=ready)
        processor._data_source_update_sink = sink

        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        valid_calls = [c for c in sink.update_status.call_args_list if c.args[0] == DataSourceState.VALID]
        assert len(valid_calls) >= 1

        await processor.stop()

    @pytest.mark.asyncio
    async def test_initialized_returns_false_before_first_poll(self):
        processor = make_processor()
        assert not processor.initialized()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_second_start_call_raises(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        processor = make_processor()
        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        # Like a thread, the polling task can only be started once
        with pytest.raises(RuntimeError):
            processor.start()

        await processor.stop()
