"""
Tests for AsyncFeatureRequesterImpl and AsyncPollingUpdateProcessor.
"""

import asyncio
import logging
import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aiohttp.client_reqrep import ConnectionKey

from ldclient.config import Config
from ldclient.impl.aio.transport_types import TransportResponse
from ldclient.impl.datasource.async_feature_requester import (
    AsyncFeatureRequesterImpl
)
from ldclient.impl.datasource.async_polling import AsyncPollingUpdateProcessor
from ldclient.impl.retry import (
    POLLING_RESET_SUCCESSES,
    AfterConsecutiveSuccesses,
    RetryState,
    for_polling
)
from ldclient.impl.util import UnsuccessfulResponseException
from ldclient.interfaces import (
    AsyncDataSourceUpdateSink,
    DataSourceErrorKind,
    DataSourceState
)
from ldclient.testing.mock_async_components import MockAsyncFeatureStore
from ldclient.testing.test_util import no_retry_jitter
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# Sample data returned by a successful poll
SAMPLE_FLAGS = {'flagkey': {'key': 'flagkey', 'version': 1, 'deleted': False}}
SAMPLE_SEGMENTS = {'segkey': {'key': 'segkey', 'version': 1, 'deleted': False}}
SAMPLE_DATA = {FEATURES: SAMPLE_FLAGS, SEGMENTS: SAMPLE_SEGMENTS}


def make_config(**kwargs):
    """Create a Config with the test SDK key."""
    return Config('SDK_KEY', **kwargs)


# aiohttp's connection errors read the connection key when they are turned
# into a string, which the data source does, so a real one is needed here.
_CONNECTION_KEY = ConnectionKey(
    host='app.launchdarkly.com',
    port=443,
    is_ssl=True,
    ssl=True,
    proxy=None,
    proxy_auth=None,
    proxy_headers_hash=None,
    server_hostname=None,
)


def fast_retry_state(delay=0.001):
    """A retry state with tiny delays, so a test does not have to wait out the
    real extended-regime delay of five minutes."""
    return RetryState(
        initial_delay=delay,
        normal_ceiling=delay,
        extended_initial_delay=delay,
        extended_ceiling=delay,
        reset_policy=AfterConsecutiveSuccesses(POLLING_RESET_SUCCESSES),
        operating_cadence=delay,
    )


def make_processor(config=None, store=None, ready=None, requester=None, retry_state=None):
    if config is None:
        config = make_config()
    if store is None:
        store = MockAsyncFeatureStore()
    if ready is None:
        ready = asyncio.Event()
    if requester is None:
        requester = MagicMock()
        requester.close = AsyncMock()
    return AsyncPollingUpdateProcessor(
        config=config,
        requester=requester,
        store=store,
        ready=ready,
        retry_state=retry_state,
    )


def make_transport(*responses: TransportResponse):
    """Create a stub transport whose request() returns the given responses in order."""
    transport = MagicMock()
    transport.request = AsyncMock(side_effect=list(responses))
    return transport


class TestAsyncFeatureRequesterImpl:
    @pytest.mark.asyncio
    async def test_successful_response_returns_flags_and_segments(self):
        import json
        config = make_config()
        transport = make_transport(
            TransportResponse(200, {}, json.dumps({'flags': SAMPLE_FLAGS, 'segments': SAMPLE_SEGMENTS}))
        )
        requester = AsyncFeatureRequesterImpl(config, transport)

        data = await requester.get_all_data()

        assert data[FEATURES] == SAMPLE_FLAGS
        assert data[SEGMENTS] == SAMPLE_SEGMENTS

    @pytest.mark.asyncio
    async def test_304_not_modified_returns_cached_data(self):
        from ldclient.impl.datasource.async_feature_requester import CacheEntry

        config = make_config()
        transport = make_transport(TransportResponse(304, {}, ''))
        requester = AsyncFeatureRequesterImpl(config, transport)

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
        requester = AsyncFeatureRequesterImpl(config, transport)

        await requester.get_all_data()

        cache_entry = requester._cache.get(requester._poll_uri)
        assert cache_entry is not None
        assert cache_entry.etag == '"v1"'

    @pytest.mark.asyncio
    async def test_http_error_raises_unsuccessful_response_exception(self):
        config = make_config()
        transport = make_transport(TransportResponse(401, {}, ''))
        requester = AsyncFeatureRequesterImpl(config, transport)

        with pytest.raises(UnsuccessfulResponseException) as exc_info:
            await requester.get_all_data()

        assert exc_info.value.status == 401

    @pytest.mark.asyncio
    async def test_payload_filter_key_appended_to_uri(self):
        config = Config('SDK_KEY', payload_filter_key='my-filter')
        requester = AsyncFeatureRequesterImpl(config, MagicMock())

        assert 'filter=my-filter' in requester._poll_uri

    @pytest.mark.asyncio
    async def test_close_closes_owned_transport(self):
        with patch('ldclient.impl.datasource.async_feature_requester.AsyncHTTPTransport') as MockTransport:
            MockTransport.return_value.close = AsyncMock()
            requester = AsyncFeatureRequesterImpl(make_config())  # no transport -> owns one

            await requester.close()

            MockTransport.return_value.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_does_not_close_injected_transport(self):
        transport = MagicMock()
        transport.close = AsyncMock()
        requester = AsyncFeatureRequesterImpl(make_config(), transport)

        await requester.close()

        transport.close.assert_not_called()


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
    async def test_unexpected_http_error_keeps_polling_and_leaves_ready_unset(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready, retry_state=fast_retry_state())

        mock_requester = AsyncMock(side_effect=UnsuccessfulResponseException(401))
        processor._requester.get_all_data = mock_requester

        processor.start()
        await asyncio.sleep(0.1)

        # A rejected SDK key must not falsely unblock initialization, and it
        # must not stop the poller.
        assert not ready.is_set()
        assert not processor.initialized()
        assert mock_requester.call_count >= 2

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_unexpected_http_error_moves_to_the_extended_regime(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        retry = fast_retry_state()
        processor = make_processor(retry_state=retry)
        processor._requester.get_all_data = AsyncMock(side_effect=UnsuccessfulResponseException(401))

        processor.start()
        await asyncio.sleep(0.05)

        assert retry.in_extended_regime

        await processor.stop()

    @pytest.mark.asyncio
    async def test_the_first_success_after_an_outage_polls_at_the_cadence(self):
        # RETRY 1.4.8: a backoff wait applies to a retry, not to every
        # operation. _poll returns the wait, so this reads it directly rather
        # than measuring elapsed time.
        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        with no_retry_jitter():
            retry = for_polling(30)
            processor = make_processor(config=config, store=store, ready=ready, retry_state=retry)

            processor._requester.get_all_data = AsyncMock(side_effect=UnsuccessfulResponseException(401))
            await processor._fetch_and_store()
            assert retry.next_delay == 5 * 60

            processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)
            await processor._fetch_and_store()
            assert retry.next_delay == 30
            assert retry.in_extended_regime, "one success restores the cadence but does not reset"

            await processor._fetch_and_store()
            assert retry.next_delay == 30
            assert not retry.in_extended_regime, "two successes in a row reset the state"

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

        # A generic error must NOT release start_wait; _ready stays unset until a
        # later poll succeeds. Wait until the store is actually initialized
        # (call_count reaches 3) to verify polling kept running.
        async def wait_for_initialized():
            while not store.initialized:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_initialized(), timeout=2.0)

        assert ready.is_set()
        assert call_count >= 3

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_general_exception_does_not_set_ready(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()
        processor = make_processor(config=config, store=store, ready=ready)

        async def get_all_data():
            raise RuntimeError("transient error")

        processor._requester.get_all_data = get_all_data

        processor.start()
        # Let several polls run; all raise. A transient error must not release
        # start_wait (mirrors sync, which leaves _ready unset here).
        await asyncio.sleep(0.05)

        assert not ready.is_set()
        assert not processor.initialized()

        await processor.stop()

    @pytest.mark.parametrize(
        "error",
        [
            aiohttp.ClientConnectorCertificateError(
                _CONNECTION_KEY, ssl.SSLCertVerificationError("self-signed certificate")
            ),
            aiohttp.ClientConnectorSSLError(_CONNECTION_KEY, OSError("handshake failed")),
            ssl.SSLEOFError("EOF occurred in violation of protocol"),
            ConnectionResetError(104, "reset by peer"),
        ],
        ids=["aiohttp-certificate", "aiohttp-tls", "peer-close-handshake", "reset"],
    )
    @pytest.mark.asyncio
    async def test_transport_failures_poll_again_at_the_cadence(self, error):
        """No transport failure reaches the extended regime, an aiohttp
        certificate failure included. Only an HTTP status can do that."""
        retry = for_polling(30)
        processor = make_processor(retry_state=retry)
        processor._requester.get_all_data = AsyncMock(side_effect=error)

        await processor._fetch_and_store()
        assert retry.next_delay == 30
        assert not retry.in_extended_regime

    @pytest.mark.asyncio
    async def test_the_log_reports_the_growing_retry_delay(self, caplog):
        """The message has to carry the real delay, so someone reading logs can
        see the backoff working."""
        caplog.set_level(logging.WARNING)

        with no_retry_jitter():
            retry = for_polling(30)
            processor = make_processor(retry_state=retry)
            processor._requester.get_all_data = AsyncMock(side_effect=UnsuccessfulResponseException(401))

            await processor._fetch_and_store()
            await processor._fetch_and_store()

        messages = [r.getMessage() for r in caplog.records]
        assert messages == [
            "Received HTTP error 401 (invalid SDK key) for polling request - will retry in 300.0s",
            "Received HTTP error 401 (invalid SDK key) for polling request - will retry in 600.0s",
        ]
        # An error a person has to fix is logged at error level, every time.
        assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.ERROR]

    @pytest.mark.asyncio
    async def test_a_transport_error_reports_a_delay_and_keeps_its_stacktrace(self, caplog):
        caplog.set_level(logging.WARNING)

        with no_retry_jitter():
            retry = for_polling(30)
            processor = make_processor(retry_state=retry)
            processor._requester.get_all_data = AsyncMock(side_effect=ConnectionResetError(104, "reset by peer"))

            await processor._fetch_and_store()

        record = caplog.records[0]
        assert record.getMessage() == "Error encountered when updating flags: [Errno 104] reset by peer - will retry in 30.0s"
        # The handler has exited by the time this is logged, so the exception
        # has to be carried explicitly for the traceback to survive.
        assert record.exc_info is not None

    @pytest.mark.asyncio
    async def test_stop_closes_requester(self):
        processor = make_processor()

        await processor.stop()

        processor._requester.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_awaits_poll_before_closing_transport(self):
        # The in-flight poll must finish unwinding before the transport is
        # closed, so we never close it out from under a live request.
        order = []
        started = asyncio.Event()

        async def slow_poll():
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                order.append('poll_done')
                raise

        async def close():
            order.append('transport_closed')

        requester = MagicMock()
        requester.get_all_data = slow_poll
        requester.close = close

        processor = make_processor(requester=requester)
        processor.start()
        await asyncio.wait_for(started.wait(), timeout=1.0)

        await processor.stop()

        assert order == ['poll_done', 'transport_closed']

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
    async def test_unexpected_error_updates_sink_to_interrupted_never_off(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()

        sink = MagicMock(spec=AsyncDataSourceUpdateSink)
        config._data_source_update_sink = sink

        processor = make_processor(config=config, store=store, ready=ready, retry_state=fast_retry_state())
        processor._data_source_update_sink = sink

        processor._requester.get_all_data = AsyncMock(
            side_effect=UnsuccessfulResponseException(403)
        )

        processor.start()
        await asyncio.sleep(0.05)

        interrupted = [c for c in sink.update_status.call_args_list if c.args[0] == DataSourceState.INTERRUPTED]
        assert len(interrupted) >= 1
        error_info = interrupted[0].args[1]
        assert error_info.kind == DataSourceErrorKind.ERROR_RESPONSE
        assert error_info.status_code == 403

        assert not any(c.args[0] == DataSourceState.OFF for c in sink.update_status.call_args_list)

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_stop_updates_sink_to_off(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        config = make_config()
        sink = MagicMock(spec=AsyncDataSourceUpdateSink)
        config._data_source_update_sink = sink

        processor = make_processor(config=config)
        processor._data_source_update_sink = sink
        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        await processor.stop()

        assert any(c.args[0] == DataSourceState.OFF for c in sink.update_status.call_args_list)

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_valid_status_is_reported_before_ready_is_set(self, mock_interval):
        # Mirrors go-server-sdk#442: a caller that wakes on readiness must not
        # still be able to read INITIALIZING.
        mock_interval.__get__ = MagicMock(return_value=0)

        from ldclient.impl.datasource.async_status import (
            AsyncDataSourceUpdateSinkImpl
        )
        from ldclient.impl.listeners import Listeners

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        observed = []
        listeners = Listeners()
        listeners.add(lambda status: observed.append((status.state, ready.is_set())))

        config = make_config()
        config._data_source_update_sink = AsyncDataSourceUpdateSinkImpl(store, listeners, Listeners())

        processor = make_processor(config=config, store=store, ready=ready)
        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        assert observed[0] == (DataSourceState.VALID, False)

        await processor.stop()

    @pytest.mark.asyncio
    @patch('ldclient.config.Config.poll_interval', new_callable=MagicMock)
    async def test_successful_poll_updates_sink_to_valid(self, mock_interval):
        mock_interval.__get__ = MagicMock(return_value=0)

        store = MockAsyncFeatureStore()
        ready = asyncio.Event()
        config = make_config()

        # The sink's init() also marks the underlying store initialized, so the
        # ready event fires (it gates on store.initialized) and ready.wait()
        # below completes.
        sink = MagicMock(spec=AsyncDataSourceUpdateSink)

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
    async def test_second_start_call_is_a_no_op(self, mock_interval):
        # AsyncLDClient.start() is documented as an idempotent no-op, so
        # nothing underneath it may raise on a repeat call.
        mock_interval.__get__ = MagicMock(return_value=0)

        processor = make_processor()
        processor._requester.get_all_data = AsyncMock(return_value=SAMPLE_DATA)

        processor.start()
        first_task = processor._task
        processor.start()

        assert processor._task is first_task

        await processor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_transport_when_cancelled_mid_wait(self):
        # If the caller of stop() is cancelled while it waits for the poll to
        # finish, the owned transport must still be closed (the close is in a
        # finally), rather than leaking.
        requester = MagicMock()
        requester.close = AsyncMock()
        processor = make_processor(requester=requester)

        # Replace the task's wait so it hangs until we cancel stop().
        waiting = asyncio.Event()

        async def hang():
            waiting.set()
            await asyncio.Event().wait()

        processor._task.wait_stopped = hang  # type: ignore[method-assign]

        stop_task = asyncio.create_task(processor.stop())
        await asyncio.wait_for(waiting.wait(), timeout=2.0)
        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop_task

        requester.close.assert_awaited_once()
