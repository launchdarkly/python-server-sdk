"""
Tests for flag overrides through the async client. These mirror the key scenarios of the sync
client tests.
"""
import asyncio
import threading
import time
from typing import Any, Dict, List, Optional

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.integrations.files.filedata import make_flag_with_value
from ldclient.interfaces import AsyncFeatureStore, DataStoreMode
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.mock_async_components import MockAsyncEventProcessor
from ldclient.testing.mock_components import (
    FailingOverrideSourceBuilder,
    MockDataSourceBuilder,
    MockOverrideSource,
    StaticInitializer
)
from ldclient.versioned_data_kind import FEATURES

user = Context.create('user-key')

# A context whose key is empty is invalid.
invalid_context = Context.create('')


class AsyncHangingSynchronizer:
    """An async synchronizer that connects but never yields data."""

    def __init__(self):
        self._stop = AsyncEvent()

    @property
    def name(self) -> str:
        return "AsyncHangingSynchronizer"

    async def sync(self, ss):
        await self._stop.wait()
        return
        yield

    async def stop(self) -> None:
        self._stop.set()


class AsyncStaticInitializer(StaticInitializer):
    async def fetch(self, ss):  # type: ignore[override]
        return super().fetch(ss)


class UnavailableAsyncStore(AsyncInMemoryFeatureStore):
    """A persistent store whose every read fails, as during a store outage."""

    async def is_initialized(self) -> bool:
        raise RuntimeError("store unreachable")

    async def get(self, kind, key):
        raise RuntimeError("store unreachable")

    async def all(self, kind):
        raise RuntimeError("store unreachable")


class UninitializedAsyncStore(AsyncInMemoryFeatureStore):
    """A persistent store that holds data but was never initialized by an SDK."""

    @property
    def initialized(self) -> bool:
        return False

    async def is_initialized(self) -> bool:
        return False


async def uninitialized_store_holding(key: str) -> UninitializedAsyncStore:
    store = UninitializedAsyncStore()
    await store.upsert(FEATURES, FlagBuilder(key).version(1).on(False).off_variation(0).variations('ld-value').build().to_json_dict())
    return store


def single_value_flag(key: str, value: Any) -> dict:
    return make_flag_with_value(key, value).to_json_dict()


def recorded_keys(client: AsyncLDClient) -> List[str]:
    """The flag keys of the evaluations the client handed to the event processor."""
    processor: Any = client._event_processor
    return [event.key for event in processor.events]


async def make_uninitialized_client(source: MockOverrideSource, store: Optional[AsyncFeatureStore] = None) -> AsyncLDClient:
    """
    A client whose data system can never obtain LaunchDarkly data, with the given override
    source and, when given, a read-only persistent store.
    """
    datasystem = AsyncDataSystemConfig(synchronizers=[MockDataSourceBuilder(AsyncHangingSynchronizer())], override_source=source.builder, data_store=store, data_store_mode=DataStoreMode.READ_ONLY)
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor())
    client = AsyncLDClient(config)
    await client.start(start_wait=0)
    return client


async def make_initialized_client(flags: Dict[str, dict], source: MockOverrideSource, segments: Optional[Dict[str, dict]] = None) -> AsyncLDClient:
    initializer = AsyncStaticInitializer(flags, segments or {})
    datasystem = AsyncDataSystemConfig(initializers=[MockDataSourceBuilder(initializer)], override_source=source.builder)
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor())
    client = AsyncLDClient(config)
    await client.start(start_wait=5)
    assert await client.is_initialized() is True
    return client


@pytest.mark.asyncio
async def test_override_is_served_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        assert await client.is_initialized() is False
        detail = await client.variation_detail('overridden-flag', user, False)
        assert detail.value is True
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
    finally:
        await client.close()
    assert source.close_count == 1


@pytest.mark.asyncio
async def test_non_overridden_flag_still_short_circuits_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        detail = await client.variation_detail('other-flag', user, False)
        assert detail.value is False
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_override_removal_restores_short_circuit():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        assert await client.variation('overridden-flag', user, False) is True
        source.set_overrides({}, {})
        detail = await client.variation_detail('overridden-flag', user, False)
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('overrides', [{}, {'other-flag': single_value_flag('other-flag', True)}], ids=['empty-layer', 'layer-without-the-key'])
async def test_not_initialized_client_returns_not_ready_for_an_invalid_context_when_the_layer_lacks_the_key(overrides):
    client = await make_uninitialized_client(MockOverrideSource(flags=overrides))
    try:
        detail = await client.variation_detail('requested-flag', invalid_context, False)
        # The not-ready handling runs before the context check, as it does without overrides,
        # and records the evaluation of the unknown flag.
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'})
        assert recorded_keys(client) == ['requested-flag']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_not_initialized_client_checks_the_context_before_serving_an_override():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        detail = await client.variation_detail('overridden-flag', invalid_context, False)
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'USER_NOT_SPECIFIED'})
        assert recorded_keys(client) == []
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('store_outage', [True, False], ids=['store-outage', 'uninitialized-store'])
async def test_not_initialized_client_returns_not_ready_when_the_store_has_no_launchdarkly_data_and_the_layer_lacks_the_key(store_outage):
    store: AsyncFeatureStore = UnavailableAsyncStore() if store_outage else await uninitialized_store_holding('requested-flag')
    source = MockOverrideSource(flags={'other-flag': single_value_flag('other-flag', True)})
    client = await make_uninitialized_client(source, store=store)
    try:
        detail = await client.variation_detail('requested-flag', user, False)
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'})
        assert recorded_keys(client) == ['requested-flag']
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_override_is_served_during_a_persistent_store_outage():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source, store=UnavailableAsyncStore())
    try:
        detail = await client.variation_detail('overridden-flag', user, False)
        assert detail.value is True
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_all_flags_state_is_invalid_when_the_store_is_uninitialized_and_the_layer_is_empty():
    client = await make_uninitialized_client(MockOverrideSource(), store=await uninitialized_store_holding('ld-flag'))
    try:
        state = await client.all_flags_state(user)
        assert state.valid is False
        assert state.to_values_map() == {}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_override_source_start_failure_fails_start_and_closes_the_source():
    source = MockOverrideSource(start_error=RuntimeError("cannot start"))
    datasystem = AsyncDataSystemConfig(synchronizers=[MockDataSourceBuilder(AsyncHangingSynchronizer())], override_source=source.builder)
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor())
    client = AsyncLDClient(config)
    with pytest.raises(RuntimeError):
        await client.start(start_wait=0)
    assert source.close_count == 1


@pytest.mark.asyncio
async def test_invalid_override_source_configuration_fails_construction():
    datasystem = AsyncDataSystemConfig(synchronizers=[MockDataSourceBuilder(AsyncHangingSynchronizer())], override_source=FailingOverrideSourceBuilder())
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor())
    with pytest.raises(ValueError):
        AsyncLDClient(config)


@pytest.mark.asyncio
async def test_override_takes_precedence_over_launchdarkly_data_and_all_flags_reflects_it():
    ld_flag = FlagBuilder('flag-precedence').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').build().to_json_dict()
    source = MockOverrideSource(flags={'flag-precedence': single_value_flag('flag-precedence', 'override-value')})
    client = await make_initialized_client({'flag-precedence': ld_flag, 'flag-normal': normal}, source)
    try:
        detail = await client.variation_detail('flag-precedence', user, 'default')
        assert detail.value == 'override-value'
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
        detail = await client.variation_detail('flag-normal', user, 'default')
        assert detail.reason == {'kind': 'OFF'}

        state = await client.all_flags_state(user, with_reasons=True)
        assert state.to_values_map() == {'flag-precedence': 'override-value', 'flag-normal': 'normal-value'}
        assert state.get_flag_reason('flag-precedence') == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_all_flags_state_contains_only_overrides_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        state = await client.all_flags_state(user)
        assert state.valid is True
        assert state.to_values_map() == {'overridden-flag': True}
        source.set_overrides({}, {})
        assert (await client.all_flags_state(user)).valid is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_flag_tracker_is_notified_of_override_changes_on_the_event_loop():
    source = MockOverrideSource()
    client = await make_uninitialized_client(source)
    try:
        changes: asyncio.Queue = asyncio.Queue()
        loop_thread = threading.current_thread()
        listener_threads = []

        def listener(change):
            listener_threads.append(threading.current_thread())
            changes.put_nowait(change)

        client.flag_tracker.add_listener(listener)

        # The source pushes from a worker thread, as the file source does. The notification
        # is delivered on the event loop's thread.
        await asyncio.to_thread(source.set_overrides, {'overridden-flag': single_value_flag('overridden-flag', True)}, {})
        change = await asyncio.wait_for(changes.get(), 5)
        assert change.key == 'overridden-flag'
        assert listener_threads[0] is loop_thread
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_flag_value_change_listener_sees_override_value_changes():
    ld_flag = FlagBuilder('flag').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    source = MockOverrideSource()
    client = await make_initialized_client({'flag': ld_flag}, source)
    try:
        changes: asyncio.Queue = asyncio.Queue()
        await client.flag_tracker.add_flag_value_change_listener('flag', user, changes.put_nowait)
        await asyncio.to_thread(source.set_overrides, {'flag': single_value_flag('flag', 'override-value')}, {})
        change = await asyncio.wait_for(changes.get(), 5)
        assert change.old_value == 'ld-value'
        assert change.new_value == 'override-value'
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_override_evaluation_events_carry_override_affected_marking():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = await make_uninitialized_client(source)
    try:
        assert await client.variation('overridden-flag', user, False) is True
        records = [e for e in client._event_processor.events if isinstance(e, EventInputEvaluation)]
        assert [e.key for e in records] == ['overridden-flag']
        assert records[0].override_affected is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_all_flags_state_turns_off_event_tracking_for_override_affected_flags():
    overridden = FlagBuilder('overridden-flag').version(7).on(False).off_variation(0).variations(True).track_events(True).debug_events_until_date(int(time.time() * 1000) + 100000).build().to_json_dict()
    plain = FlagBuilder('plain-tracked').version(1).on(False).off_variation(0).variations(True).track_events(True).build().to_json_dict()
    source = MockOverrideSource(flags={'overridden-flag': overridden})
    client = await make_initialized_client({'plain-tracked': plain}, source)
    try:
        state = await client.all_flags_state(user, with_reasons=True)
        flags_state = state.to_json_dict()['$flagsState']
        assert flags_state['plain-tracked']['trackEvents'] is True
        assert 'trackEvents' not in flags_state['overridden-flag']
        assert 'debugEventsUntilDate' not in flags_state['overridden-flag']
        assert flags_state['overridden-flag']['reason']['overrideAffected'] is True
        assert state.get_flag_value('overridden-flag') is True
    finally:
        await client.close()
