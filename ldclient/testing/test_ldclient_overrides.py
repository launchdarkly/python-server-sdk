"""
Tests for flag overrides through the client: the override source lifecycle, the overlay at the
store read boundary, the not-initialized gate, the all-flags state, and flag change notifications.
"""
import json
import logging
import time
from queue import Empty, Queue
from typing import Any, Dict, List, Optional

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.datasystem import custom
from ldclient.evaluation import EvaluationDetail
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.hook import EvaluationSeriesContext, Hook, Metadata
from ldclient.impl.datasystem.fdv1 import FDv1
from ldclient.impl.events.event_processor import DefaultEventProcessor
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.integrations.files.filedata import make_flag_with_value
from ldclient.interfaces import (
    DataSourceState,
    DataStoreMode,
    FeatureStore,
    FlagChange
)
from ldclient.migrations import Stage
from ldclient.testing.builders import (
    FlagBuilder,
    FlagRuleBuilder,
    SegmentBuilder,
    make_clause_matching_segment_key
)
from ldclient.testing.mock_components import (
    FailingOverrideSourceBuilder,
    HangingSynchronizer,
    MockOverrideSource,
    StaticInitializer
)
from ldclient.testing.stub_util import MockEventProcessor, MockHttp
from ldclient.versioned_data_kind import FEATURES

user = Context.create('user-key')

# A context whose key is empty is invalid.
invalid_context = Context.create('')


def single_value_flag(key: str, value: Any) -> dict:
    return make_flag_with_value(key, value).to_json_dict()


def make_uninitialized_client(source: MockOverrideSource, store: Optional[FeatureStore] = None) -> LDClient:
    """
    A client whose data system can never obtain LaunchDarkly data, with the given override
    source and, when given, a read-only persistent store.
    """
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(source.builder)
    if store is not None:
        datasystem.data_store(store, DataStoreMode.READ_ONLY)
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem.build(), event_processor_class=MockEventProcessor)
    return LDClient(config, start_wait=0)


def make_initialized_client(flags: Dict[str, dict], source: MockOverrideSource, segments: Optional[Dict[str, dict]] = None) -> LDClient:
    """A client that has initialized with the given LaunchDarkly data, with the given override source."""
    initializer = StaticInitializer(flags, segments or {})
    datasystem = custom().initializers([initializer.builder]).overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)
    client = LDClient(config, start_wait=5)
    assert client.is_initialized() is True
    return client


def warnings_containing(caplog, text: str):
    return [r for r in caplog.records if r.levelno == logging.WARNING and text in r.getMessage()]


def recorded_keys(client: LDClient) -> List[str]:
    """The flag keys of the evaluations the client handed to the event processor."""
    processor: Any = client._event_processor
    return [event.key for event in processor._events]


class UnavailableStore(InMemoryFeatureStore):
    """A persistent store whose every read fails, as during a store outage."""

    @property
    def initialized(self) -> bool:
        raise RuntimeError("store unreachable")

    def get(self, kind, key, callback=lambda x: x):
        raise RuntimeError("store unreachable")

    def all(self, kind, callback=lambda x: x):
        raise RuntimeError("store unreachable")


class UninitializedStore(InMemoryFeatureStore):
    """A persistent store that holds data but was never initialized by an SDK."""

    @property
    def initialized(self) -> bool:
        return False


def uninitialized_store_holding(key: str) -> UninitializedStore:
    store = UninitializedStore()
    store.upsert(FEATURES, FlagBuilder(key).version(1).on(False).off_variation(0).variations('ld-value').build().to_json_dict())
    return store


def test_override_is_served_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        assert client.is_initialized() is False
        detail = client.variation_detail('overridden-flag', user, False)
        assert detail.value is True
        assert detail.variation_index == 0
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}


def test_non_overridden_flag_still_short_circuits_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        detail = client.variation_detail('other-flag', user, False)
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'})


def test_override_removal_restores_short_circuit():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        assert client.variation('overridden-flag', user, False) is True
        source.set_overrides({}, {})
        detail = client.variation_detail('overridden-flag', user, False)
        assert detail.value is False
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}


@pytest.mark.parametrize('overrides', [{}, {'other-flag': single_value_flag('other-flag', True)}], ids=['empty-layer', 'layer-without-the-key'])
def test_not_initialized_client_returns_not_ready_for_an_invalid_context_when_the_layer_lacks_the_key(overrides):
    with make_uninitialized_client(MockOverrideSource(flags=overrides)) as client:
        detail = client.variation_detail('requested-flag', invalid_context, False)
        # The not-ready handling runs before the context check, as it does without overrides,
        # and records the evaluation of the unknown flag.
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'})
        assert recorded_keys(client) == ['requested-flag']


def test_not_initialized_client_checks_the_context_before_serving_an_override():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        detail = client.variation_detail('overridden-flag', invalid_context, False)
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'USER_NOT_SPECIFIED'})
        assert recorded_keys(client) == []


@pytest.mark.parametrize('make_store', [UnavailableStore, lambda: uninitialized_store_holding('requested-flag')], ids=['store-outage', 'uninitialized-store'])
def test_not_initialized_client_returns_not_ready_when_the_store_has_no_launchdarkly_data_and_the_layer_lacks_the_key(make_store):
    source = MockOverrideSource(flags={'other-flag': single_value_flag('other-flag', True)})
    with make_uninitialized_client(source, store=make_store()) as client:
        detail = client.variation_detail('requested-flag', user, False)
        assert detail == EvaluationDetail(False, None, {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'})
        assert recorded_keys(client) == ['requested-flag']


def test_override_is_served_during_a_persistent_store_outage():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source, store=UnavailableStore()) as client:
        detail = client.variation_detail('overridden-flag', user, False)
        assert detail.value is True
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}


def test_all_flags_state_is_invalid_when_the_store_is_uninitialized_and_the_layer_is_empty():
    with make_uninitialized_client(MockOverrideSource(), store=uninitialized_store_holding('ld-flag')) as client:
        state = client.all_flags_state(user)
        assert state.valid is False
        assert state.to_values_map() == {}


def test_override_source_is_started_before_the_constructor_returns():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    client = make_uninitialized_client(source)
    try:
        assert source.start_count == 1
        assert client.variation('overridden-flag', user, False) is True
    finally:
        client.close()


def test_override_source_is_closed_when_the_client_is_closed():
    source = MockOverrideSource()
    client = make_uninitialized_client(source)
    assert source.close_count == 0
    client.close()
    assert source.close_count == 1


def test_invalid_override_source_configuration_fails_construction():
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(FailingOverrideSourceBuilder()).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)
    with pytest.raises(ValueError):
        LDClient(config, start_wait=0)


def test_override_source_start_failure_fails_construction_and_stops_the_started_components():
    stopped = []

    class RecordingEventProcessor(MockEventProcessor):
        def stop(self):
            stopped.append(True)

    source = MockOverrideSource(start_error=RuntimeError("cannot start"))
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=RecordingEventProcessor)
    with pytest.raises(RuntimeError):
        LDClient(config, start_wait=0)
    # The source and the event processor that were set up before the failure are closed.
    assert source.close_count == 1
    assert stopped == [True]


def test_override_source_is_not_started_when_offline():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    datasystem = custom().overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, offline=True)
    with LDClient(config, start_wait=0) as client:
        assert source.start_count == 0
        assert client.variation('overridden-flag', user, False) is False


def test_data_system_without_override_source_has_no_override_layer():
    datasystem = custom().synchronizers(HangingSynchronizer().builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)
    with LDClient(config, start_wait=0) as client:
        assert client._data_system.override_layer is None
        detail = client.variation_detail('any-flag', user, False)
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}


def test_legacy_data_system_has_no_override_layer():
    config = Config(sdk_key='SDK_KEY', offline=True)
    with LDClient(config) as client:
        assert isinstance(client._data_system, FDv1)
        assert client._data_system.override_layer is None


def test_override_takes_precedence_over_launchdarkly_data():
    ld_flag = FlagBuilder('flag-precedence').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').build().to_json_dict()
    source = MockOverrideSource(flags={'flag-precedence': single_value_flag('flag-precedence', 'override-value')})
    with make_initialized_client({'flag-precedence': ld_flag, 'flag-normal': normal}, source) as client:
        detail = client.variation_detail('flag-precedence', user, 'default')
        assert detail.value == 'override-value'
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
        detail = client.variation_detail('flag-normal', user, 'default')
        assert detail.value == 'normal-value'
        assert detail.reason == {'kind': 'OFF'}


def test_full_flag_override_evaluates_targeting_rules_and_referenced_override_segment():
    segment_flag = FlagBuilder('flag-segment').version(1).on(True).off_variation(0).fallthrough_variation(0).variations('not-included', 'included').rules(
        FlagRuleBuilder().id('segment-rule').variation(1).clauses(make_clause_matching_segment_key('overridden-segment')).build()
    ).build().to_json_dict()
    ld_segment = SegmentBuilder('overridden-segment').version(100).build().to_json_dict()
    override_segment = SegmentBuilder('overridden-segment').version(101).included(user.key).build().to_json_dict()
    source = MockOverrideSource(flags={'flag-segment': segment_flag}, segments={'overridden-segment': override_segment})
    with make_initialized_client({}, source, segments={'overridden-segment': ld_segment}) as client:
        detail = client.variation_detail('flag-segment', user, 'default')
        assert detail.value == 'included'
        assert detail.reason == {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'segment-rule', 'overrideAffected': True}


def test_overridden_prerequisite_marks_the_dependent_flag():
    dependent = FlagBuilder('dependent').version(1).on(True).off_variation(0).fallthrough_variation(1).variations('prereq-failed', 'affected-value').prerequisite('prereq', 1).build().to_json_dict()
    ld_prereq = FlagBuilder('prereq').version(100).on(False).off_variation(0).variations('a', 'b').build().to_json_dict()
    override_prereq = FlagBuilder('prereq').version(200).on(True).off_variation(0).fallthrough_variation(1).variations('a', 'b').build().to_json_dict()
    source = MockOverrideSource(flags={'prereq': override_prereq})
    with make_initialized_client({'dependent': dependent, 'prereq': ld_prereq}, source) as client:
        detail = client.variation_detail('dependent', user, 'default')
        assert detail.value == 'affected-value'
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}


def test_all_flags_state_contains_only_overrides_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        state = client.all_flags_state(user)
        assert state.valid is True
        assert state.to_values_map() == {'overridden-flag': True}


def test_all_flags_state_overrides_only_warning_is_logged_once(caplog):
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        with caplog.at_level(logging.WARNING):
            assert client.all_flags_state(user).valid is True
            assert client.all_flags_state(user).valid is True
        assert len(warnings_containing(caplog, 'Returning only flags from the override layer')) == 1


def test_all_flags_state_is_invalid_when_not_initialized_and_override_layer_is_empty():
    source = MockOverrideSource()
    with make_uninitialized_client(source) as client:
        state = client.all_flags_state(user)
        assert state.valid is False
        assert state.to_values_map() == {}


def test_all_flags_state_reflects_overrides_when_initialized():
    ld_flag = FlagBuilder('flag-precedence').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').build().to_json_dict()
    source = MockOverrideSource(flags={
        'flag-precedence': single_value_flag('flag-precedence', 'override-value'),
        'override-only': single_value_flag('override-only', 'only-value'),
    })
    with make_initialized_client({'flag-precedence': ld_flag, 'flag-normal': normal}, source) as client:
        state = client.all_flags_state(user, with_reasons=True)
        assert state.valid is True
        assert state.to_values_map() == {'flag-precedence': 'override-value', 'flag-normal': 'normal-value', 'override-only': 'only-value'}
        assert state.get_flag_reason('flag-precedence') == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
        assert state.get_flag_reason('flag-normal') == {'kind': 'OFF'}


def test_flag_tracker_is_notified_of_override_changes():
    source = MockOverrideSource()
    with make_uninitialized_client(source) as client:
        changes: Queue = Queue()
        client.flag_tracker.add_listener(lambda change: changes.put(change))

        source.set_overrides({'overridden-flag': single_value_flag('overridden-flag', True)}, {})
        change = changes.get(timeout=5)
        assert isinstance(change, FlagChange)
        assert change.key == 'overridden-flag'

        # An identical snapshot notifies nothing.
        source.set_overrides({'overridden-flag': single_value_flag('overridden-flag', True)}, {})
        with pytest.raises(Empty):
            changes.get(timeout=0.3)

        source.set_overrides({}, {})
        assert changes.get(timeout=5).key == 'overridden-flag'


def test_flag_value_change_listener_sees_override_value_changes():
    ld_flag = FlagBuilder('flag').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    source = MockOverrideSource()
    with make_initialized_client({'flag': ld_flag}, source) as client:
        changes: Queue = Queue()
        client.flag_tracker.add_flag_value_change_listener('flag', user, lambda change: changes.put(change))

        source.set_overrides({'flag': single_value_flag('flag', 'override-value')}, {})
        change = changes.get(timeout=5)
        assert change.old_value == 'ld-value'
        assert change.new_value == 'override-value'

        source.set_overrides({}, {})
        change = changes.get(timeout=5)
        assert change.old_value == 'override-value'
        assert change.new_value == 'ld-value'


def test_data_source_status_is_unaffected_by_overrides():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        assert client.is_initialized() is False
        assert client.data_source_status_provider.status.state == DataSourceState.INITIALIZING


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

def evaluation_events_by_key(client: LDClient) -> Dict[str, EventInputEvaluation]:
    """The evaluation records the client handed to the event processor, keyed by flag key."""
    records = {}
    processor: Any = client._event_processor
    for event in processor._events:
        if isinstance(event, EventInputEvaluation):
            records[event.key] = event
    return records


def test_override_evaluation_events_carry_override_affected_marking():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        assert client.variation('overridden-flag', user, False) is True
        records = evaluation_events_by_key(client)
        assert list(records.keys()) == ['overridden-flag']
        assert records['overridden-flag'].override_affected is True


def test_ordinary_evaluation_events_are_not_marked():
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').track_events(True).build().to_json_dict()
    source = MockOverrideSource(flags={'other': single_value_flag('other', True)})
    with make_initialized_client({'flag-normal': normal}, source) as client:
        client.variation('flag-normal', user, 'default')
        records = evaluation_events_by_key(client)
        assert records['flag-normal'].override_affected is False
        assert records['flag-normal'].track_events is True


def tracked_bool_flag(key: str) -> FlagBuilder:
    return FlagBuilder(key).version(100).variations(False, True).off_variation(0).fallthrough_variation(1).track_events(True)


def test_overridden_prerequisite_marks_the_dependent_evaluation_records():
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
    with make_initialized_client(ld_data, source) as client:
        detail = client.variation_detail('top-flag', user, False)
        assert detail.value is True
        assert detail.reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}

        records = evaluation_events_by_key(client)
        assert sorted(records.keys()) == ['leaf-flag', 'mid-flag', 'plain-flag', 'top-flag']
        assert records['top-flag'].override_affected is True
        assert records['mid-flag'].override_affected is True
        assert records['leaf-flag'].override_affected is True
        assert records['plain-flag'].override_affected is False
        assert records['mid-flag'].prereq_of is not None and records['mid-flag'].prereq_of.key == 'top-flag'
        assert records['leaf-flag'].prereq_of is not None and records['leaf-flag'].prereq_of.key == 'mid-flag'
        assert records['leaf-flag'].reason == {'kind': 'FALLTHROUGH', 'overrideAffected': True}
        assert records['plain-flag'].reason == {'kind': 'FALLTHROUGH'}


def test_all_flags_state_turns_off_event_tracking_for_override_affected_flags():
    debug_until = int(time.time() * 1000) + 100000
    ld_data = {
        'plain-tracked': FlagBuilder('plain-tracked').version(1).on(False).off_variation(0).variations(True).track_events(True).debug_events_until_date(debug_until).build().to_json_dict(),
        'dependent-tracked': FlagBuilder('dependent-tracked').version(1).on(True).variations(False, True).fallthrough_variation(1).prerequisite('overridden-flag', 0).track_events(True).debug_events_until_date(debug_until).build().to_json_dict(),
    }
    # The overridden flag is on and serves variation 0, so the dependent flag's prerequisite passes.
    overridden = FlagBuilder('overridden-flag').version(7).on(True).fallthrough_variation(0).variations(True).track_events(True).debug_events_until_date(debug_until).build().to_json_dict()
    source = MockOverrideSource(flags={'overridden-flag': overridden})
    with make_initialized_client(ld_data, source) as client:
        state = client.all_flags_state(user, with_reasons=True)
        assert state.valid is True
        flags_state = state.to_json_dict()['$flagsState']

        # A flag with no override keeps its tracking fields.
        assert flags_state['plain-tracked']['trackEvents'] is True
        assert flags_state['plain-tracked']['debugEventsUntilDate'] == debug_until

        # The overridden flag and the flag that depends on it stay in the state with their values
        # and marked reasons, but with no tracking fields.
        for key in ('overridden-flag', 'dependent-tracked'):
            assert flags_state[key]['reason']['overrideAffected'] is True, key
            assert 'trackEvents' not in flags_state[key], key
            assert 'trackReason' not in flags_state[key], key
            assert 'debugEventsUntilDate' not in flags_state[key], key
        assert state.get_flag_value('overridden-flag') is True
        assert state.get_flag_value('dependent-tracked') is True
        assert flags_state['overridden-flag']['version'] == 7


def test_all_flags_state_omits_details_of_override_affected_flags_when_details_only_for_tracked_flags():
    # With details only for tracked flags, an override-affected flag counts as untracked, so its
    # version and reason are omitted like any other untracked flag, and its value stays.
    overridden = FlagBuilder('overridden-flag').version(7).on(False).off_variation(0).variations(True).track_events(True).build().to_json_dict()
    source = MockOverrideSource(flags={'overridden-flag': overridden})
    with make_initialized_client({}, source) as client:
        state = client.all_flags_state(user, with_reasons=True, details_only_for_tracked_flags=True)
        flags_state = state.to_json_dict()['$flagsState']
        assert state.get_flag_value('overridden-flag') is True
        assert 'version' not in flags_state['overridden-flag']
        assert 'reason' not in flags_state['overridden-flag']


def test_wrong_type_result_of_overridden_flag_stays_marked():
    details = []

    class CapturingHook(Hook):
        @property
        def metadata(self) -> Metadata:
            return Metadata(name='capturing-hook')

        def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
            return data

        def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
            details.append(detail)
            return data

    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', 'not-a-stage')})
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor, hooks=[CapturingHook()])
    with LDClient(config, start_wait=0) as client:
        stage, _ = client.migration_variation('overridden-flag', user, Stage.OFF)
        assert stage == Stage.OFF
        assert len(details) == 1
        assert details[0].value == 'off'
        assert details[0].reason == {'kind': 'ERROR', 'errorKind': 'WRONG_TYPE', 'overrideAffected': True}


def raise_evaluation_failure(*args):
    raise RuntimeError("evaluation failure")


def test_failed_evaluation_is_marked_only_when_the_flag_came_from_the_override_layer(monkeypatch):
    source = MockOverrideSource(flags={'overridden-flag': tracked_bool_flag('overridden-flag').on(True).build().to_json_dict()})
    with make_initialized_client({'plain-flag': tracked_bool_flag('plain-flag').on(True).build().to_json_dict()}, source) as client:
        monkeypatch.setattr(client._evaluator, 'evaluate', raise_evaluation_failure)

        # The failure of the override flag is marked. The failure of the ordinary flag is not.
        detail = client.variation_detail('overridden-flag', user, 'default')
        assert detail == EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION', 'overrideAffected': True})
        detail = client.variation_detail('plain-flag', user, 'default')
        assert detail == EvaluationDetail('default', None, {'kind': 'ERROR', 'errorKind': 'EXCEPTION'})

        # The marked record produces no individual event. The ordinary record keeps its tracking.
        records = evaluation_events_by_key(client)
        assert records['overridden-flag'].override_affected is True
        assert records['plain-flag'].override_affected is False
        assert records['plain-flag'].track_events is True


def test_all_flags_state_turns_off_event_tracking_for_an_override_flag_whose_evaluation_fails(monkeypatch):
    source = MockOverrideSource(flags={'overridden-flag': tracked_bool_flag('overridden-flag').on(True).build().to_json_dict()})
    with make_initialized_client({'plain-flag': tracked_bool_flag('plain-flag').on(True).build().to_json_dict()}, source) as client:
        monkeypatch.setattr(client._evaluator, 'evaluate', raise_evaluation_failure)
        state = client.all_flags_state(user, with_reasons=True)

        # The failed override flag stays in the state with a marked reason and no tracking
        # fields. The failed ordinary flag keeps its tracking fields.
        assert state.valid is True
        flags_state = state.to_json_dict()['$flagsState']
        assert flags_state['overridden-flag']['reason'] == {'kind': 'ERROR', 'errorKind': 'EXCEPTION', 'overrideAffected': True}
        assert 'trackEvents' not in flags_state['overridden-flag']
        assert flags_state['plain-flag']['reason'] == {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
        assert flags_state['plain-flag']['trackEvents'] is True


def test_override_affected_evaluations_appear_only_in_summary_output():
    # End to end through the real event processor: the overridden flag requests individual
    # feature events and debug events, and an ordinary flag requests feature events.
    debug_until = int(time.time() * 1000) + 100000
    overridden = FlagBuilder('flag-tracked-override').version(300).on(False).off_variation(0).variations('override-value').track_events(True).debug_events_until_date(debug_until).build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').track_events(True).build().to_json_dict()
    source = MockOverrideSource(flags={'flag-tracked-override': overridden})
    mock_http = MockHttp()

    initializer = StaticInitializer({'flag-normal': normal}, {})
    datasystem = custom().initializers([initializer.builder]).overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, diagnostic_opt_out=True, event_processor_class=lambda config: DefaultEventProcessor(config, mock_http))
    with LDClient(config, start_wait=5) as client:
        assert client.is_initialized() is True
        for _ in range(2):
            assert client.variation('flag-tracked-override', user, 'default1') == 'override-value'
        assert client.variation('flag-normal', user, 'default2') == 'normal-value'
        client.flush()
        client._event_processor._wait_until_inactive()

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
