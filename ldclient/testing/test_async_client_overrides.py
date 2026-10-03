"""
Tests for flag overrides through the async client. These mirror the key scenarios of the sync
client tests.
"""
import asyncio
import json
import threading
import time
from typing import Any, Dict, List, Optional

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.hook import AsyncHook, EvaluationSeriesContext, Metadata
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.events.async_event_processor import (
    DefaultAsyncEventProcessor
)
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.integrations.files.filedata import make_flag_with_value
from ldclient.interfaces import AsyncFeatureStore, DataStoreMode
from ldclient.migrations import Stage
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.impl.events.test_async_event_processor import MockAioHttp
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


@pytest.mark.asyncio
async def test_override_source_is_not_started_when_offline():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    config = AsyncConfig('SDK_KEY', datasystem_config=AsyncDataSystemConfig(override_source=source.builder), offline=True)
    client = AsyncLDClient(config)
    await client.start(start_wait=0)
    try:
        assert source.start_count == 0
        assert await client.variation('overridden-flag', user, False) is False
    finally:
        await client.close()


def test_override_update_after_the_client_and_its_loop_are_gone_is_dropped():
    source = MockOverrideSource()

    async def run_client() -> None:
        client = await make_uninitialized_client(source)
        client.flag_tracker.add_listener(lambda change: None)
        await client.close()

    asyncio.run(run_client())
    # A reload that finishes after the client closed finds the event loop closed. The
    # notification has nobody left to reach and is dropped.
    source.set_overrides({'overridden-flag': single_value_flag('overridden-flag', True)}, {})


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def tracked_bool_flag(key: str) -> FlagBuilder:
    return FlagBuilder(key).version(100).variations(False, True).off_variation(0).fallthrough_variation(1).track_events(True)


def evaluation_events_by_key(client: AsyncLDClient) -> Dict[str, EventInputEvaluation]:
    """The evaluation records the client handed to the event processor, keyed by flag key."""
    processor: Any = client._event_processor
    return {event.key: event for event in processor.events if isinstance(event, EventInputEvaluation)}


async def raise_evaluation_failure(*args):
    raise RuntimeError("evaluation failure")


@pytest.mark.asyncio
async def test_overridden_prerequisite_marks_the_dependent_evaluation_records():
    # top-flag (LaunchDarkly) --> mid-flag (LaunchDarkly) --> leaf-flag (overridden)
    #                         --> plain-flag (LaunchDarkly)
    # The LaunchDarkly copy of leaf-flag is off, so the chain passes only through the override.
    ld_data = {
        'top-flag': tracked_bool_flag('top-flag').on(True).prerequisite('mid-flag', 1).prerequisite('plain-flag', 1).build().to_json_dict(),
        'mid-flag': tracked_bool_flag('mid-flag').on(True).prerequisite('leaf-flag', 1).build().to_json_dict(),
        'plain-flag': tracked_bool_flag('plain-flag').on(True).build().to_json_dict(),
        'leaf-flag': tracked_bool_flag('leaf-flag').on(False).build().to_json_dict(),
    }
    source = MockOverrideSource(flags={'leaf-flag': tracked_bool_flag('leaf-flag').on(True).build().to_json_dict()})
    client = await make_initialized_client(ld_data, source)
    try:
        detail = await client.variation_detail('top-flag', user, False)
        assert detail.value is True
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}

        records = evaluation_events_by_key(client)
        assert sorted(records.keys()) == ['leaf-flag', 'mid-flag', 'plain-flag', 'top-flag']
        assert records['top-flag'].override_affected is True
        assert records['mid-flag'].override_affected is True
        assert records['leaf-flag'].override_affected is True
        assert records['plain-flag'].override_affected is False
        assert records['leaf-flag'].reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
        assert records['plain-flag'].reason == {'kind': 'FALLTHROUGH'}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_wrong_type_result_of_overridden_flag_stays_marked():
    details = []

    class CapturingHook(AsyncHook):
        @property
        def metadata(self) -> Metadata:
            return Metadata(name='capturing-hook')

        async def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
            return data

        async def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
            details.append(detail)
            return data

    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', 'not-a-stage')})
    datasystem = AsyncDataSystemConfig(synchronizers=[MockDataSourceBuilder(AsyncHangingSynchronizer())], override_source=source.builder)
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor(), hooks=[CapturingHook()])
    client = AsyncLDClient(config)
    await client.start(start_wait=0)
    try:
        stage, _ = await client.migration_variation('overridden-flag', user, Stage.OFF)
        assert stage == Stage.OFF
        assert len(details) == 1
        assert details[0].value == 'off'
        assert details[0].reason == {'kind': 'ERROR', 'errorKind': 'WRONG_TYPE', 'overrideAffected': True}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_failed_evaluation_is_marked_only_when_the_flag_came_from_the_override_layer(monkeypatch):
    source = MockOverrideSource(flags={'overridden-flag': tracked_bool_flag('overridden-flag').on(True).build().to_json_dict()})
    client = await make_initialized_client({'plain-flag': tracked_bool_flag('plain-flag').on(True).build().to_json_dict()}, source)
    try:
        monkeypatch.setattr(client._evaluator, 'evaluate', raise_evaluation_failure)

        # The failure of the override flag is marked. The failure of the ordinary flag is not.
        detail = await client.variation_detail('overridden-flag', user, 'default')
        assert detail == EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION', 'overrideAffected': True})
        detail = await client.variation_detail('plain-flag', user, 'default')
        assert detail == EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION'})

        # The marked record produces no individual event. The ordinary record keeps its tracking.
        records = evaluation_events_by_key(client)
        assert records['overridden-flag'].override_affected is True
        assert records['plain-flag'].override_affected is False
        assert records['plain-flag'].track_events is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_all_flags_state_turns_off_event_tracking_for_an_override_flag_whose_evaluation_fails(monkeypatch):
    source = MockOverrideSource(flags={'overridden-flag': tracked_bool_flag('overridden-flag').on(True).build().to_json_dict()})
    client = await make_initialized_client({'plain-flag': tracked_bool_flag('plain-flag').on(True).build().to_json_dict()}, source)
    try:
        monkeypatch.setattr(client._evaluator, 'evaluate', raise_evaluation_failure)
        state = await client.all_flags_state(user, with_reasons=True)

        # The failed override flag stays in the state with a marked reason and no tracking
        # fields. The failed ordinary flag keeps its tracking fields.
        assert state.valid is True
        flags_state = state.to_json_dict()['$flagsState']
        assert flags_state['overridden-flag']['reason'] == {'kind': 'ERROR', 'errorKind': 'EXCEPTION', 'overrideAffected': True}
        assert 'trackEvents' not in flags_state['overridden-flag']
        assert flags_state['plain-flag']['reason'] == {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
        assert flags_state['plain-flag']['trackEvents'] is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_override_affected_evaluations_appear_only_in_summary_output():
    # End to end through the real event processor: the overridden flag requests individual
    # feature events and debug events, and an ordinary flag requests feature events.
    debug_until = int(time.time() * 1000) + 100000
    overridden = FlagBuilder('flag-tracked-override').version(300).on(False).off_variation(0).variations('override-value').track_events(True).debug_events_until_date(debug_until).build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').track_events(True).build().to_json_dict()
    source = MockOverrideSource(flags={'flag-tracked-override': overridden})
    mock_http = MockAioHttp()

    initializer = AsyncStaticInitializer({'flag-normal': normal}, {})
    datasystem = AsyncDataSystemConfig(initializers=[MockDataSourceBuilder(initializer)], override_source=source.builder)
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, diagnostic_opt_out=True, event_processor_class=lambda config: DefaultAsyncEventProcessor(config, mock_http))
    client = AsyncLDClient(config)
    await client.start(start_wait=5)
    try:
        assert await client.is_initialized() is True
        for _ in range(2):
            assert await client.variation('flag-tracked-override', user, 'default1') == 'override-value'
        assert await client.variation('flag-normal', user, 'default2') == 'normal-value'
        assert await client._event_processor.flush_and_wait(5) is True
    finally:
        await client.close()

    assert mock_http.request_data is not None
    output = json.loads(mock_http.request_data)
    kinds = sorted(e['kind'] for e in output)
    assert kinds == ['feature', 'index', 'summary']
    feature = [e for e in output if e['kind'] == 'feature'][0]
    assert feature['key'] == 'flag-normal'
    summary = [e for e in output if e['kind'] == 'summary'][0]
    assert summary['features']['flag-tracked-override']['default'] == 'default1'
    assert summary['features']['flag-tracked-override']['counters'] == [
        {'count': 2, 'value': 'override-value', 'variation': 0, 'version': 300, 'overrideAffected': True}
    ]
    assert summary['features']['flag-normal']['counters'] == [{'count': 1, 'value': 'normal-value', 'variation': 0, 'version': 100}]
