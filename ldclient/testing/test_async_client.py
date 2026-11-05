"""
Tests for AsyncLDClient.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.context import Context
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import DataStoreMode
from ldclient.testing.mock_async_components import (
    MockAsyncEventProcessor,
    MockAsyncFeatureStore
)
from ldclient.versioned_data_kind import FEATURES


def _offline_config(**kwargs):
    """Return an AsyncConfig that uses offline mode to avoid any network connections."""
    return AsyncConfig("test-sdk-key", offline=True, **kwargs)


def _test_data_config(td=None, **kwargs):
    """Return an AsyncConfig whose FDv2 data system syncs from TestDataV2."""
    if td is None:
        td = TestDataV2.data_source()
    return AsyncConfig(
        "test-sdk-key",
        datasystem_config=AsyncDataSystemConfig(synchronizers=[td.async_builder]),
        **kwargs,
    )


def _daemon_config(store, **kwargs):
    """Return an AsyncConfig whose FDv2 data system reads only from the given store."""
    return AsyncConfig(
        "test-sdk-key",
        datasystem_config=AsyncDataSystemConfig(data_store=store, data_store_mode=DataStoreMode.READ_ONLY),
        **kwargs,
    )


def _make_flag(key: str, value, version: int = 1) -> dict:
    """Build a minimal feature flag dict usable with AsyncInMemoryFeatureStore."""
    return {
        'key': key,
        'version': version,
        'on': True,
        'variations': [False, True, value],
        'fallthrough': {'variation': 2},
        'offVariation': 0,
        'targets': [],
        'rules': [],
        'prerequisites': [],
        'salt': 'abc',
        'deleted': False,
    }


@pytest.mark.asyncio
async def test_default_variation_returns_default_when_not_started():
    """variation() before start() returns the default value."""
    client = AsyncLDClient(_offline_config())
    context = Context.create('user-1')
    result = await client.variation('some-flag', context, 'fallback')
    # In offline mode the client behaves as initialized — no network required.
    # The flag doesn't exist, so CLIENT_NOT_READY or FLAG_NOT_FOUND — either way default is returned.
    assert result == 'fallback'


@pytest.mark.asyncio
async def test_variation_returns_flag_value_when_initialized():
    """After start(), variation() returns the flag value from the data source.

    TestDataV2 supplies the flag without any network connection, so the
    client initializes as soon as the synchronizer delivers its basis.
    """
    td = TestDataV2.data_source()
    td.update(td.flag('my-flag').value_for_all('hello'))

    client = AsyncLDClient(_test_data_config(td, send_events=False))
    await client.start(start_wait=1.0)
    assert await client.is_initialized()

    context = Context.create('user-1')
    result = await client.variation('my-flag', context, 'default')
    assert result == 'hello'

    await client.close()


@pytest.mark.asyncio
async def test_start_is_idempotent():
    """Calling start() twice does not raise and does not double-initialize."""
    client = AsyncLDClient(_offline_config())
    await client.start()
    data_system_after_first = client._data_system

    await client.start()
    data_system_after_second = client._data_system

    assert data_system_after_second is data_system_after_first
    await client.close()


@pytest.mark.asyncio
async def test_close_is_idempotent():
    """Calling close() twice does not raise."""
    client = AsyncLDClient(_offline_config())
    await client.start()
    await client.close()
    await client.close()


@pytest.mark.asyncio
async def test_context_manager():
    """async with AsyncLDClient(config) as client: starts and closes the client."""
    async with AsyncLDClient(_offline_config()) as client:
        assert await client.is_initialized()
    assert client._closed is True


@pytest.mark.asyncio
async def test_flush_delegates_to_event_processor():
    """flush() calls flush() on the underlying event processor."""
    client = AsyncLDClient(_test_data_config(send_events=False))
    await client.start()

    # Replace the event processor with a mock that tracks flush calls
    mock_ep = MagicMock()
    mock_ep.flush = MagicMock(return_value=None)
    mock_ep.stop = AsyncMock()
    client._event_processor = mock_ep

    await client.flush()
    mock_ep.flush.assert_called_once()

    await client.close()


@pytest.mark.asyncio
async def test_flush_is_noop_when_offline():
    """flush() returns without touching the event processor in offline mode."""
    client = AsyncLDClient(_offline_config())
    await client.start()

    mock_ep = MagicMock()
    mock_ep.flush = MagicMock(return_value=None)
    mock_ep.stop = AsyncMock()
    client._event_processor = mock_ep

    await client.flush()
    mock_ep.flush.assert_not_called()

    await client.close()


@pytest.mark.asyncio
async def test_migration_variation_returns_default_stage():
    """migration_variation() returns the default stage and a tracker when the flag is missing."""
    from ldclient.migrations import OpTracker, Stage

    async with AsyncLDClient(_offline_config()) as client:
        stage, tracker = await client.migration_variation('flag', Context.create('user'), Stage.LIVE)

    assert stage == Stage.LIVE
    assert isinstance(tracker, OpTracker)


@pytest.mark.asyncio
async def test_hooks_are_invoked_during_variation():
    """Hooks added via add_hook() have before/after called during variation()."""
    from ldclient.hook import AsyncHook, Metadata

    class RecordingHook(AsyncHook):
        def __init__(self):
            self.before_calls = []
            self.after_calls = []

        @property
        def metadata(self):
            return Metadata(name='recording-hook')

        async def before_evaluation(self, series_context, data):
            self.before_calls.append(series_context)
            return data

        async def after_evaluation(self, series_context, data, detail):
            self.after_calls.append((series_context, detail))
            return data

    hook = RecordingHook()
    # Register the hook via add_hook() after construction.
    client = AsyncLDClient(_offline_config())
    client.add_hook(hook)
    async with client:
        context = Context.create('user-1')
        result = await client.variation('some-flag', context, 'default-val')

    assert result == 'default-val'
    assert len(hook.before_calls) == 1
    assert hook.before_calls[0].key == 'some-flag'
    assert len(hook.after_calls) == 1
    assert hook.after_calls[0][0].key == 'some-flag'


@pytest.mark.asyncio
async def test_add_hook_rejects_sync_hook():
    """add_hook() raises TypeError when given a synchronous Hook."""
    from ldclient.hook import EvaluationSeriesContext, Hook, Metadata

    class SyncHook(Hook):
        @property
        def metadata(self):
            return Metadata(name='sync-hook')

        def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
            return data

        def after_evaluation(self, series_context, data, detail):
            return data

    client = AsyncLDClient(_offline_config())
    with pytest.raises(TypeError):
        client.add_hook(SyncHook())


@pytest.mark.asyncio
async def test_flag_tracker_available_before_start():
    """flag_tracker is available before start(); the data system it reads from is
    built in __init__ and not rebuilt by start(), so early listeners stay wired."""
    client = AsyncLDClient(_offline_config())

    tracker = client.flag_tracker
    assert tracker is not None
    tracker.add_listener(lambda change: None)

    data_system_before = client._data_system
    await client.start()
    assert client._data_system is data_system_before
    await client.close()


@pytest.mark.asyncio
async def test_variation_detail_returns_reason():
    """variation_detail() returns an EvaluationDetail with a non-None reason."""
    td = TestDataV2.data_source()
    td.update(td.flag('detail-flag').value_for_all('hello'))

    async with AsyncLDClient(_test_data_config(td, send_events=False)) as client:
        context = Context.create('user-1')
        detail = await client.variation_detail('detail-flag', context, 'fallback')

    from ldclient.evaluation import EvaluationDetail
    assert isinstance(detail, EvaluationDetail)
    assert detail.reason is not None


@pytest.mark.asyncio
async def test_track_sends_event():
    """track() sends a custom event to the event processor."""
    mock_ep = MockAsyncEventProcessor()
    config = _test_data_config(
        event_processor_class=lambda _cfg: mock_ep,
        send_events=True,
    )
    async with AsyncLDClient(config) as client:
        context = Context.create('user-1')
        client.track('my-event', context, {'data': 1}, 3.14)

    assert len(mock_ep.events) == 1
    event = mock_ep.events[0]
    # Events are EventInputCustom objects with .key attribute
    from ldclient.impl.events.types import EventInputCustom
    assert isinstance(event, EventInputCustom)
    assert event.key == 'my-event'


@pytest.mark.asyncio
async def test_data_source_status_provider_accessible():
    """data_source_status_provider is not None after start()."""
    async with AsyncLDClient(_test_data_config(send_events=False)) as client:
        assert client.data_source_status_provider is not None


@pytest.mark.asyncio
async def test_is_offline_reflects_config():
    """is_offline() returns True when Config is created with offline=True."""
    async with AsyncLDClient(_offline_config()) as client:
        assert client.is_offline() is True

    async with AsyncLDClient(_test_data_config(send_events=False)) as client:
        assert client.is_offline() is False


@pytest.mark.asyncio
async def test_hooks_data_isolation():
    """Each hook's before_evaluation receives its own isolated {} — not data from a prior hook."""
    from ldclient.hook import AsyncHook, Metadata

    received_data_by_hook = {}

    class IsolationHook(AsyncHook):
        def __init__(self, name, inject_key=None, inject_val=None):
            self._name = name
            self._inject_key = inject_key
            self._inject_val = inject_val

        @property
        def metadata(self):
            return Metadata(name=self._name)

        async def before_evaluation(self, series_context, data):
            # Record a copy of what we received
            received_data_by_hook[self._name] = dict(data)
            if self._inject_key:
                data[self._inject_key] = self._inject_val
            return data

        async def after_evaluation(self, series_context, data, detail):
            return data

    hook_a = IsolationHook('hook-a', inject_key='hook_a', inject_val=True)
    hook_b = IsolationHook('hook-b')

    client = AsyncLDClient(_offline_config())
    client.add_hook(hook_a)
    client.add_hook(hook_b)
    async with client:
        context = Context.create('user-1')
        await client.variation('some-flag', context, 'default-val')

    # hook_a received an empty dict
    assert received_data_by_hook['hook-a'] == {}
    # hook_b also received an empty dict — not hook_a's mutated dict
    assert received_data_by_hook['hook-b'] == {}


@pytest.mark.asyncio
async def test_start_after_close_is_noop():
    """Calling start() after close() logs a warning and does nothing; the client stays closed."""
    client = AsyncLDClient(_offline_config())
    await client.start()
    await client.close()
    # Does not raise; the client remains closed.
    await client.start()
    assert client._closed is True


@pytest.mark.asyncio
async def test_start_is_single_shot_after_failure(monkeypatch):
    """If start() raises, the client is marked spent; a retry is a logged no-op."""
    client = AsyncLDClient(_offline_config())

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    # Make __start_up raise early so the start fails.
    monkeypatch.setattr("ldclient.async_client.get_plugin_hooks", boom)

    with pytest.raises(RuntimeError):
        await client.start()
    # A failed start marks the client started (spent) and closed (torn down).
    assert client._started is True
    assert client._closed is True

    # Retry does not re-run start-up (it would raise again if it did).
    await client.start()


@pytest.mark.asyncio
async def test_start_cleans_up_and_propagates_on_cancellation(monkeypatch):
    """A cancelled start() runs cleanup (CancelledError is a BaseException),
    marks the client spent, and re-raises so cancellation still propagates."""
    client = AsyncLDClient(_offline_config())

    cleaned = False
    original_cleanup = client._close_components

    async def spy_cleanup(*args, **kwargs):
        nonlocal cleaned
        cleaned = True
        await original_cleanup(*args, **kwargs)

    client._close_components = spy_cleanup

    def cancel(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr("ldclient.async_client.get_plugin_hooks", cancel)

    with pytest.raises(asyncio.CancelledError):
        await client.start()

    # Cleanup ran (stopping the started components), the instance is spent
    # (single-shot via _started), it is closed, and the CancelledError
    # propagated.
    assert cleaned is True
    assert client._started is True
    assert client._closed is True


@pytest.mark.asyncio
async def test_all_flags_state_returns_flag_values():
    """all_flags_state() returns a valid state with each flag's value."""
    store = MockAsyncFeatureStore()
    # init() decodes the flag dicts into model objects, matching the real
    # data-source flow that all_flags_state() reads back.
    await store.init({FEATURES: {
        'flag-a': _make_flag('flag-a', 'value-a'),
        'flag-b': _make_flag('flag-b', 'value-b'),
    }})

    async with AsyncLDClient(_daemon_config(store, send_events=False)) as client:
        context = Context.create('user-1')
        state = await client.all_flags_state(context)

    assert state.valid
    assert state.to_values_map() == {'flag-a': 'value-a', 'flag-b': 'value-b'}


@pytest.mark.asyncio
async def test_all_flags_state_degrades_gracefully_when_evaluator_raises():
    """A per-flag evaluation error degrades only that flag: the payload is not
    aborted, and the failed flag does not reuse a neighbor's prerequisites."""
    from ldclient.evaluation import EvaluationDetail
    from ldclient.impl.evaluator_common import EvalResult

    store = MockAsyncFeatureStore()
    # Insertion order is preserved: bad-first tests that a first-flag failure
    # does not raise UnboundLocalError; good-then-bad tests that the trailing
    # failed flag does not inherit the good flag's prerequisites.
    await store.init({FEATURES: {
        'flag-bad-first': _make_flag('flag-bad-first', 'x'),
        'flag-good': _make_flag('flag-good', 'value-good'),
        'flag-bad-last': _make_flag('flag-bad-last', 'y'),
    }})

    async with AsyncLDClient(_daemon_config(store, send_events=False)) as client:
        async def fake_evaluate(flag, context, event_factory):
            if flag['key'].startswith('flag-bad'):
                raise RuntimeError("boom")
            result = EvalResult()
            result.detail = EvaluationDetail('value-good', 2, {'kind': 'FALLTHROUGH'})
            result.prerequisites = ['prereq-x']
            return result

        client._evaluator.evaluate = fake_evaluate
        context = Context.create('user-1')
        state = await client.all_flags_state(context, with_reasons=True)

    # The payload was not aborted by the first flag raising.
    assert state.valid
    # The good flag still evaluated normally.
    assert state.get_flag_value('flag-good') == 'value-good'
    # The failed flags degraded to None rather than raising.
    assert state.get_flag_value('flag-bad-first') is None
    assert state.get_flag_value('flag-bad-last') is None

    # The trailing failed flag did not inherit the good flag's prerequisites.
    flags_state = state.to_json_dict()['$flagsState']
    assert flags_state['flag-good'].get('prerequisites') == ['prereq-x']
    assert 'prerequisites' not in flags_state['flag-bad-last']
    assert 'prerequisites' not in flags_state['flag-bad-first']


@pytest.mark.asyncio
async def test_cached_data_warnings_are_logged_once_per_client(caplog):
    caplog.set_level(logging.WARNING, logger='ldclient.util')
    store = MockAsyncFeatureStore()
    await store.force_set(FEATURES, _make_flag('my-flag', 'hello'))
    # Daemon mode reports cached data, so every evaluation reads last known values.
    client = AsyncLDClient(_daemon_config(store, send_events=False))
    await client.start(start_wait=1.0)
    assert await client.is_initialized()
    context = Context.create('user-1')

    assert await client.variation('my-flag', context, 'default') == 'hello'
    assert await client.variation('my-flag', context, 'default') == 'hello'
    assert (await client.all_flags_state(context)).valid
    assert (await client.all_flags_state(context)).valid

    warnings = [r.message for r in caplog.records if r.levelname == 'WARNING']
    eval_warnings = [m for m in warnings if m.startswith('Feature Flag evaluation attempted')]
    all_flags_warnings = [m for m in warnings if m.startswith('all_flags_state() called before')]
    assert len(eval_warnings) == 1
    assert len(all_flags_warnings) == 1
    assert eval_warnings[0].endswith('This message is logged once.')
    assert all_flags_warnings[0].endswith('This message is logged once.')

    await client.close()
