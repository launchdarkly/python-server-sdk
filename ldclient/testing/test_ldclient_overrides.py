"""
Tests for flag overrides through the client: the override source lifecycle, the overlay at the
store read boundary, the not-initialized gate, the all-flags state, and flag change notifications.
"""
import logging
from queue import Empty, Queue
from typing import Any, Dict, Optional

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.datasystem import custom
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.datasystem.fdv1 import FDv1
from ldclient.impl.integrations.files.filedata import make_flag_with_value
from ldclient.interfaces import DataSourceState, FlagChange
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
from ldclient.testing.stub_util import MockEventProcessor

user = Context.create('user-key')


def single_value_flag(key: str, value: Any) -> dict:
    return make_flag_with_value(key, value).to_json_dict()


def make_uninitialized_client(source: MockOverrideSource) -> LDClient:
    """A client whose data system can never obtain LaunchDarkly data, with the given override source."""
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)
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


def test_override_is_served_when_client_is_not_initialized():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    with make_uninitialized_client(source) as client:
        assert client.is_initialized() is False
        detail = client.variation_detail('overridden-flag', user, False)
        assert detail.value is True
        assert detail.variation_index == 0
        assert detail.reason == {'kind': 'OFF', 'overrideAffected': True}


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


def test_override_source_is_not_started_when_offline():
    source = MockOverrideSource(flags={'overridden-flag': single_value_flag('overridden-flag', True)})
    datasystem = custom().overrides(source.builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, offline=True)
    with LDClient(config, start_wait=0) as client:
        assert source.start_count == 0
        assert client.variation('overridden-flag', user, False) is False


def test_data_system_without_override_source_reports_none_configured():
    datasystem = custom().synchronizers(HangingSynchronizer().builder).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)
    with LDClient(config, start_wait=0) as client:
        assert client._data_system.override_source_configured is False
        detail = client.variation_detail('any-flag', user, False)
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}


def test_legacy_data_system_reports_no_override_source():
    config = Config(sdk_key='SDK_KEY', offline=True)
    with LDClient(config) as client:
        assert isinstance(client._data_system, FDv1)
        assert client._data_system.override_source_configured is False


def test_override_takes_precedence_over_launchdarkly_data():
    ld_flag = FlagBuilder('flag-precedence').version(100).on(False).off_variation(0).variations('ld-value').build().to_json_dict()
    normal = FlagBuilder('flag-normal').version(100).on(False).off_variation(0).variations('normal-value').build().to_json_dict()
    source = MockOverrideSource(flags={'flag-precedence': single_value_flag('flag-precedence', 'override-value')})
    with make_initialized_client({'flag-precedence': ld_flag, 'flag-normal': normal}, source) as client:
        detail = client.variation_detail('flag-precedence', user, 'default')
        assert detail.value == 'override-value'
        assert detail.reason == {'kind': 'OFF', 'overrideAffected': True}
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
        assert state.get_flag_reason('flag-precedence') == {'kind': 'OFF', 'overrideAffected': True}
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
