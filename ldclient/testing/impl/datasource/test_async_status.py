import pytest

from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.impl.datasource.async_status import (
    AsyncDataSourceStatusProviderImpl,
    AsyncDataSourceUpdateSinkImpl
)
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatus,
    FlagChange
)
from ldclient.testing.builders import FlagBuilder, SegmentBuilder
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_sink(store=None):
    if store is None:
        store = AsyncInMemoryFeatureStore()
    status_listeners = Listeners()
    flag_change_listeners = Listeners()
    sink = AsyncDataSourceUpdateSinkImpl(store, status_listeners, flag_change_listeners)
    return sink, status_listeners, flag_change_listeners


def make_flag(key, version=1):
    return FlagBuilder(key).version(version).build()


def make_segment(key, version=1):
    return SegmentBuilder(key).version(version).build()


class FlagChangeCapture:
    """Captures FlagChange events delivered to a listener."""

    def __init__(self):
        self.keys = []

    def __call__(self, change: FlagChange):
        self.keys.append(change.key)


class StatusCapture:
    """Captures DataSourceStatus events delivered to a status listener."""

    def __init__(self):
        self.statuses = []

    def __call__(self, status: DataSourceStatus):
        self.statuses.append(status)


# ---------------------------------------------------------------------------
# init tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_init_notifies_all_flag_change_listeners():
    sink, _, flag_listeners = make_sink()

    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag_a = make_flag('flag-a')
    flag_b = make_flag('flag-b')

    await sink.init({
        FEATURES: {
            'flag-a': flag_a.to_json_dict(),
            'flag-b': flag_b.to_json_dict(),
        },
        SEGMENTS: {},
    })

    assert sorted(capture.keys) == ['flag-a', 'flag-b']


@pytest.mark.asyncio
async def test_init_does_not_notify_flag_listeners_when_none_registered():
    sink, _, flag_listeners = make_sink()

    # No listener added — calling init should not raise and should be a no-op
    # for notifications.
    flag_a = make_flag('flag-a')
    await sink.init({FEATURES: {'flag-a': flag_a.to_json_dict()}, SEGMENTS: {}})
    # No assertions needed — just confirming no error and no listener calls.


@pytest.mark.asyncio
async def test_init_notifies_listeners_for_changed_flags_on_reinit():
    sink, _, flag_listeners = make_sink()
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag_v1 = make_flag('flag-a', version=1)
    await sink.init({FEATURES: {'flag-a': flag_v1.to_json_dict()}, SEGMENTS: {}})
    capture.keys.clear()

    flag_v2 = make_flag('flag-a', version=2)
    flag_new = make_flag('flag-b', version=1)
    await sink.init({
        FEATURES: {
            'flag-a': flag_v2.to_json_dict(),
            'flag-b': flag_new.to_json_dict(),
        },
        SEGMENTS: {},
    })

    assert sorted(capture.keys) == ['flag-a', 'flag-b']


# ---------------------------------------------------------------------------
# upsert tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_flag_notifies_listener():
    sink, _, flag_listeners = make_sink()
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag = make_flag('my-flag', version=1)
    await sink.upsert(FEATURES, flag.to_json_dict())

    assert capture.keys == ['my-flag']


@pytest.mark.asyncio
async def test_upsert_segment_does_not_notify_flag_listener():
    sink, _, flag_listeners = make_sink()
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    segment = make_segment('my-segment', version=1)
    await sink.upsert(SEGMENTS, segment.to_json_dict())

    assert capture.keys == []


@pytest.mark.asyncio
async def test_upsert_same_version_does_not_notify():
    store = AsyncInMemoryFeatureStore()
    sink, _, flag_listeners = make_sink(store)
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag_v1 = make_flag('my-flag', version=1)

    # Insert the flag at version 1.
    await sink.upsert(FEATURES, flag_v1.to_json_dict())
    assert capture.keys == ['my-flag']
    capture.keys.clear()

    # Upsert the same version — store rejects it, no notification expected.
    await sink.upsert(FEATURES, flag_v1.to_json_dict())
    assert capture.keys == []


@pytest.mark.asyncio
async def test_upsert_older_version_does_not_notify():
    store = AsyncInMemoryFeatureStore()
    sink, _, flag_listeners = make_sink(store)
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag_v2 = make_flag('my-flag', version=2)
    await sink.upsert(FEATURES, flag_v2.to_json_dict())
    capture.keys.clear()

    flag_v1 = make_flag('my-flag', version=1)
    await sink.upsert(FEATURES, flag_v1.to_json_dict())
    assert capture.keys == []


@pytest.mark.asyncio
async def test_upsert_newer_version_notifies():
    store = AsyncInMemoryFeatureStore()
    sink, _, flag_listeners = make_sink(store)
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag_v1 = make_flag('my-flag', version=1)
    await sink.upsert(FEATURES, flag_v1.to_json_dict())
    capture.keys.clear()

    flag_v2 = make_flag('my-flag', version=2)
    await sink.upsert(FEATURES, flag_v2.to_json_dict())
    assert capture.keys == ['my-flag']


# ---------------------------------------------------------------------------
# delete tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_flag_notifies_listener():
    sink, _, flag_listeners = make_sink()
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    flag = make_flag('my-flag', version=1)
    await sink.upsert(FEATURES, flag.to_json_dict())
    capture.keys.clear()

    await sink.delete(FEATURES, 'my-flag', version=2)

    assert capture.keys == ['my-flag']


@pytest.mark.asyncio
async def test_delete_segment_does_not_notify_flag_listener():
    sink, _, flag_listeners = make_sink()
    capture = FlagChangeCapture()
    flag_listeners.add(capture)

    segment = make_segment('my-segment', version=1)
    await sink.upsert(SEGMENTS, segment.to_json_dict())
    capture.keys.clear()

    await sink.delete(SEGMENTS, 'my-segment', version=2)

    assert capture.keys == []


# ---------------------------------------------------------------------------
# update_status / DataSourceStatusProvider tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_status_broadcasts_to_status_listeners():
    sink, status_listeners, _ = make_sink()
    capture = StatusCapture()
    status_listeners.add(capture)

    sink.update_status(DataSourceState.VALID, None)

    assert len(capture.statuses) == 1
    assert capture.statuses[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_update_status_interrupted_during_init_stays_initializing():
    sink, status_listeners, _ = make_sink()
    capture = StatusCapture()
    status_listeners.add(capture)

    # State starts as INITIALIZING; INTERRUPTED should be coerced back.
    sink.update_status(DataSourceState.INTERRUPTED, None)

    assert len(capture.statuses) == 0  # state unchanged → no broadcast


@pytest.mark.asyncio
async def test_update_status_no_broadcast_when_unchanged():
    sink, status_listeners, _ = make_sink()

    # Transition to VALID first.
    sink.update_status(DataSourceState.VALID, None)

    capture = StatusCapture()
    status_listeners.add(capture)

    # Calling with the same state and no error should not broadcast.
    sink.update_status(DataSourceState.VALID, None)

    assert capture.statuses == []


@pytest.mark.asyncio
async def test_update_status_broadcasts_error_even_with_same_state():
    sink, status_listeners, _ = make_sink()
    sink.update_status(DataSourceState.VALID, None)

    capture = StatusCapture()
    status_listeners.add(capture)

    error = DataSourceErrorInfo(DataSourceErrorKind.NETWORK_ERROR, 0, 0.0, "connection lost")
    sink.update_status(DataSourceState.VALID, error)

    assert len(capture.statuses) == 1
    assert capture.statuses[0].error is error


@pytest.mark.asyncio
async def test_status_provider_delegates_to_sink():
    sink, status_listeners, _ = make_sink()
    provider = AsyncDataSourceStatusProviderImpl(status_listeners, sink)

    assert provider.status.state == DataSourceState.INITIALIZING

    sink.update_status(DataSourceState.VALID, None)

    assert provider.status.state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_status_provider_add_remove_listener():
    sink, status_listeners, _ = make_sink()
    provider = AsyncDataSourceStatusProviderImpl(status_listeners, sink)

    capture = StatusCapture()
    provider.add_listener(capture)

    sink.update_status(DataSourceState.VALID, None)
    assert len(capture.statuses) == 1

    provider.remove_listener(capture)
    sink.update_status(DataSourceState.OFF, None)
    assert len(capture.statuses) == 1  # listener was removed, no new events
