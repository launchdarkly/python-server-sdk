"""
Tests for AsyncLDClient.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig
from ldclient.context import Context
from ldclient.testing.mock_async_components import (
    MockAsyncEventProcessor,
    MockAsyncFeatureStore,
    MockAsyncUpdateProcessor
)
from ldclient.versioned_data_kind import FEATURES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _offline_config(**kwargs):
    """Return an AsyncConfig that uses offline mode to avoid any network connections."""
    return AsyncConfig("test-sdk-key", offline=True, **kwargs)


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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

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
    """After start(), variation() returns the stored flag value.

    We use update_processor_class=MockAsyncUpdateProcessor so that the client
    considers itself initialized (store.initialized becomes True once the
    NullUpdateProcessor fires ready) without any network connection.
    """
    store = MockAsyncFeatureStore()
    flag = _make_flag('my-flag', 'hello')
    await store.force_set(FEATURES, flag)
    # Pre-initialize the store so is_initialized() returns True
    store._initialized = True

    # Use MockAsyncUpdateProcessor which sets ready immediately
    config = AsyncConfig(
        "test-sdk-key",
        feature_store=store,
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
    )
    client = AsyncLDClient(config)
    await client.start(start_wait=1.0)

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
    # Second close should be a no-op
    await client.close()


@pytest.mark.asyncio
async def test_context_manager():
    """async with AsyncLDClient(config) as client: starts and closes the client."""
    async with AsyncLDClient(_offline_config()) as client:
        assert client.is_initialized()
    # After exiting, closed flag should be set
    assert client._closed is True


@pytest.mark.asyncio
async def test_flush_delegates_to_event_processor():
    """flush() calls flush() on the underlying event processor."""
    config = AsyncConfig(
        "test-sdk-key",
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
    )
    client = AsyncLDClient(config)
    await client.start()

    # Replace the event processor with a mock that tracks flush calls
    mock_ep = MagicMock()
    mock_ep.flush = MagicMock(return_value=None)
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
async def test_flag_tracker_before_start_raises():
    """Accessing flag_tracker before start() raises RuntimeError."""
    client = AsyncLDClient(_offline_config())
    with pytest.raises(RuntimeError):
        _ = client.flag_tracker


@pytest.mark.asyncio
async def test_variation_detail_returns_reason():
    """variation_detail() returns an EvaluationDetail with a non-None reason."""
    store = MockAsyncFeatureStore()
    flag = _make_flag('detail-flag', 'hello')
    await store.force_set(FEATURES, flag)
    store._initialized = True

    config = AsyncConfig(
        "test-sdk-key",
        feature_store=store,
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
    )
    async with AsyncLDClient(config) as client:
        context = Context.create('user-1')
        detail = await client.variation_detail('detail-flag', context, 'fallback')

    from ldclient.evaluation import EvaluationDetail
    assert isinstance(detail, EvaluationDetail)
    assert detail.reason is not None


@pytest.mark.asyncio
async def test_track_sends_event():
    """track() sends a custom event to the event processor."""
    store = MockAsyncFeatureStore()
    store._initialized = True

    mock_ep = MockAsyncEventProcessor()
    config = AsyncConfig(
        "test-sdk-key",
        feature_store=store,
        update_processor_class=MockAsyncUpdateProcessor,
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
    store = MockAsyncFeatureStore()
    store._initialized = True

    config = AsyncConfig(
        "test-sdk-key",
        feature_store=store,
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
    )
    async with AsyncLDClient(config) as client:
        assert client.data_source_status_provider is not None


@pytest.mark.asyncio
async def test_is_offline_reflects_config():
    """is_offline() returns True when Config is created with offline=True."""
    async with AsyncLDClient(_offline_config()) as client:
        assert client.is_offline() is True

    config = AsyncConfig(
        "test-sdk-key",
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
    )
    async with AsyncLDClient(config) as client:
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
async def test_start_after_close_raises():
    """Calling start() after close() raises RuntimeError."""
    client = AsyncLDClient(_offline_config())
    await client.start()
    await client.close()
    with pytest.raises(RuntimeError):
        await client.start()
