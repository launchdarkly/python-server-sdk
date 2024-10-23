from typing import Callable, Dict

import mock
import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.status import DataSourceUpdateSinkImpl
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import DataSourceErrorKind, DataSourceState
from ldclient.testing.builders import (FlagBuilder, FlagRuleBuilder,
                                       SegmentBuilder, SegmentRuleBuilder,
                                       make_clause)
from ldclient.testing.test_util import SpyListener
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


@pytest.fixture
def basic_data() -> Dict:
    flag1 = FlagBuilder('flag1').version(1).on(False).build()
    flag2 = FlagBuilder('flag2').version(1).on(False).build()
    flag3 = (
        FlagBuilder('flag3').version(1).rules(FlagRuleBuilder().variation(0).id('rule_id').track_events(True).clauses(make_clause('user', 'segmentMatch', 'segmentMatch', 'segment2')).build()).build()
    )
    segment1 = SegmentBuilder('segment1').version(1).build()
    segment2 = SegmentBuilder('segment2').version(1).build()

    return {
        FEATURES: {
            flag1.key: flag1.to_json_dict(),
            flag2.key: flag2.to_json_dict(),
            flag3.key: flag3.to_json_dict(),
        },
        SEGMENTS: {
            segment1.key: segment1.to_json_dict(),
            segment2.key: segment2.to_json_dict(),
        },
    }


@pytest.fixture
def prereq_data() -> Dict:
    flag1 = FlagBuilder('flag1').version(1).on(False).prerequisite('flag2', 0).build()
    flag2 = FlagBuilder('flag2').version(1).on(False).prerequisite('flag3', 0).prerequisite('flag4', 0).prerequisite('flag6', 0).build()
    flag3 = FlagBuilder('flag3').version(1).on(False).build()
    flag4 = FlagBuilder('flag4').version(1).on(False).build()
    flag5 = FlagBuilder('flag5').version(1).on(False).build()
    flag6 = (
        FlagBuilder('flag6').version(1).rules(FlagRuleBuilder().variation(0).id('rule_id').track_events(True).clauses(make_clause('user', 'segmentMatch', 'segmentMatch', 'segment2')).build()).build()
    )
    segment1 = SegmentBuilder('segment1').version(1).build()
    segment2 = SegmentBuilder('segment2').version(1).rules(SegmentRuleBuilder().clauses(make_clause('user', 'segmentMatch', 'segmentMatch', 'segment1')).build()).build()

    return {
        FEATURES: {
            flag1.key: flag1.to_json_dict(),
            flag2.key: flag2.to_json_dict(),
            flag3.key: flag3.to_json_dict(),
            flag4.key: flag4.to_json_dict(),
            flag5.key: flag5.to_json_dict(),
            flag6.key: flag6.to_json_dict(),
        },
        SEGMENTS: {
            segment1.key: segment1.to_json_dict(),
            segment2.key: segment2.to_json_dict(),
        },
    }


def test_defaults_to_initializing():
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), Listeners())
    assert sink.status.state == DataSourceState.INITIALIZING


def test_interrupting_initializing_stays_initializing():
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), Listeners())
    sink.update_status(DataSourceState.INTERRUPTED, None)
    assert sink.status.state == DataSourceState.INITIALIZING
    assert sink.status.error is None


def test_listener_is_only_triggered_for_state_changes():
    spy = SpyListener()
    status_listener = Listeners()
    status_listener.add(spy)

    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), status_listener, Listeners())
    sink.update_status(DataSourceState.VALID, None)
    sink.update_status(DataSourceState.VALID, None)
    assert len(spy.statuses) == 1

    sink.update_status(DataSourceState.INTERRUPTED, None)
    sink.update_status(DataSourceState.INTERRUPTED, None)
    assert len(spy.statuses) == 2


def test_all_listeners_triggered_for_single_change():
    spy1 = SpyListener()
    spy2 = SpyListener()

    status_listener = Listeners()
    status_listener.add(spy1)
    status_listener.add(spy2)

    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), status_listener, Listeners())
    sink.update_status(DataSourceState.VALID, None)

    assert len(spy1.statuses) == 1
    assert len(spy2.statuses) == 1


def test_is_called_once_per_flag_during_init(basic_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(basic_data)

    flag1 = FlagBuilder('flag1').version(2).on(False).build()
    flag4 = FlagBuilder('flag4').version(1).on(False).build()

    spy = SpyListener()
    flag_change_listener.add(spy)
    sink.init(
        {
            FEATURES: {
                flag1.key: flag1,
                flag4.key: flag4,
            }
        }
    )

    assert len(spy.statuses) == 4
    keys = set(s.key for s in spy.statuses)  # No guaranteed order

    assert 'flag1' in keys  # Version update
    assert 'flag2' in keys  # Deleted
    assert 'flag3' in keys  # Deleted
    assert 'flag4' in keys  # Newly created


def test_upsert_triggers_flag_listener(basic_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(basic_data)

    spy = SpyListener()
    flag_change_listener.add(spy)
    sink.upsert(FEATURES, FlagBuilder('flag1').version(2).on(False).build())

    assert len(spy.statuses) == 1
    assert spy.statuses[0].key == 'flag1'


def test_delete_triggers_flag_listener(basic_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(basic_data)

    spy = SpyListener()
    flag_change_listener.add(spy)
    sink.delete(FEATURES, 'flag1', 2)

    # TODO(sc-212471): Once the store starts returning a success status on delete, the flag change
    # notification can start ignoring duplicate requests like this.
    # sink.delete(FEATURES, 'flag1', 2)

    assert len(spy.statuses) == 1
    assert spy.statuses[0].key == 'flag1'


def test_triggers_if_segment_changes(basic_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(basic_data)

    spy = SpyListener()
    flag_change_listener.add(spy)
    sink.upsert(SEGMENTS, SegmentBuilder('segment2').version(2).build())

    assert len(spy.statuses) == 1
    assert spy.statuses[0].key == 'flag3'


def test_dependency_stack_if_top_of_chain_is_changed(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.upsert(FEATURES, FlagBuilder('flag4').version(2).on(False).build())

    assert len(spy.statuses) == 3

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys
    assert 'flag4' in keys


def test_triggers_when_new_prereqs_added(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.upsert(FEATURES, FlagBuilder('flag3').version(2).on(False).prerequisite('flag4', 0).build())

    assert len(spy.statuses) == 3

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys
    assert 'flag3' in keys


def test_triggers_when_prereqs_removed(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.upsert(FEATURES, FlagBuilder('flag2').version(2).on(False).prerequisite('flag3', 0).build())

    assert len(spy.statuses) == 2

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys


def test_triggers_dependency_stack_if_top_of_chain_is_deleted(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.delete(FEATURES, 'flag4', 2)

    assert len(spy.statuses) == 3

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys
    assert 'flag4' in keys


def test_triggers_dependent_segment_is_modified(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.upsert(SEGMENTS, SegmentBuilder('segment1').version(2).build())
    # TODO(sc-212471): Once the store starts returning a success status on upsert, the flag change
    # notification can start ignoring duplicate requests like this.
    # sink.upsert(SEGMENTS, SegmentBuilder('segment1').version(2).build())

    assert len(spy.statuses) == 3

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys
    assert 'flag6' in keys


def test_triggers_if_dependent_segment_removed(prereq_data):
    flag_change_listener = Listeners()
    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), Listeners(), flag_change_listener)
    sink.init(prereq_data)

    spy = SpyListener()
    flag_change_listener.add(spy)

    sink.delete(SEGMENTS, 'segment2', 2)
    # TODO(sc-212471): Once the store starts returning a success status on delete, the flag change
    # notification can start ignoring duplicate requests like this.
    # sink.delete(SEGMENTS, 'segment2', 2)

    assert len(spy.statuses) == 3

    keys = set(s.key for s in spy.statuses)
    assert 'flag1' in keys
    assert 'flag2' in keys
    assert 'flag6' in keys


def confirm_store_error(fn: Callable[[DataSourceUpdateSinkImpl], None], expected_error: str):
    status_listeners = Listeners()

    sink = DataSourceUpdateSinkImpl(InMemoryFeatureStore(), status_listeners, Listeners())
    # Make it valid first so the error changes from initializing
    sink.update_status(DataSourceState.VALID, None)

    spy = SpyListener()
    status_listeners.add(spy)

    try:
        fn(sink)
    except (Exception,):
        pass

    assert len(spy.statuses) == 1
    assert spy.statuses[0].state == DataSourceState.INTERRUPTED
    assert spy.statuses[0].error.kind == DataSourceErrorKind.STORE_ERROR
    assert spy.statuses[0].error.message == expected_error


@mock.patch('ldclient.feature_store.InMemoryFeatureStore.init', side_effect=[Exception('cannot init')])
def test_listener_is_triggered_for_init_error(prereq_data):
    confirm_store_error(lambda sink: sink.init(prereq_data), 'cannot init')


@mock.patch('ldclient.feature_store.InMemoryFeatureStore.upsert', side_effect=[Exception('cannot upsert')])
def test_listener_is_triggered_for_upsert_error(prereq_data):
    confirm_store_error(lambda sink: sink.upsert(FEATURES, {}), 'cannot upsert')


@mock.patch('ldclient.feature_store.InMemoryFeatureStore.delete', side_effect=[Exception('cannot delete')])
def test_listener_is_triggered_for_delete_error(prereq_data):
    confirm_store_error(lambda sink: sink.delete(FEATURES, 'key', 1), 'cannot delete')
