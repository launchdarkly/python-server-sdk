import threading
import time
from typing import Callable

import pytest

from ldclient.impl.datasystem.protocolv2 import (
    ChangeType,
    IntentCode,
    ObjectKind
)
from ldclient.impl.util import _Fail, _Success
from ldclient.integrations.test_datav2 import FlagBuilderV2, TestDataV2
from ldclient.interfaces import DataSourceState

# Test Data + Data Source V2


def test_creates_valid_initializer():
    """Test that TestDataV2 creates a working initializer"""
    td = TestDataV2.data_source()
    initializer = td.build_initializer()

    result = initializer.fetch()
    assert isinstance(result, _Success)

    basis = result.value
    assert not basis.persist
    assert basis.environment_id is None
    assert basis.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(basis.change_set.changes) == 0  # No flags added yet


def test_creates_valid_synchronizer():
    """Test that TestDataV2 creates a working synchronizer"""
    td = TestDataV2.data_source()
    synchronizer = td.build_synchronizer()

    updates = []
    update_count = 0

    def collect_updates():
        nonlocal update_count
        for update in synchronizer.sync():
            updates.append(update)
            update_count += 1

            if update_count == 1:
                # Should get initial state
                assert update.state == DataSourceState.VALID
                assert update.change_set is not None
                assert update.change_set.intent_code == IntentCode.TRANSFER_FULL
                synchronizer.close()
                break

    # Start the synchronizer in a thread with timeout to prevent hanging
    sync_thread = threading.Thread(target=collect_updates)
    sync_thread.start()

    # Wait for the thread to complete with timeout
    sync_thread.join(timeout=5)

    # Ensure thread completed successfully
    if sync_thread.is_alive():
        synchronizer.close()
        sync_thread.join()
        pytest.fail("Synchronizer test timed out after 5 seconds")

    assert len(updates) == 1


def verify_flag_builder_v2(desc: str, expected_props: dict, builder_actions: Callable[[FlagBuilderV2], FlagBuilderV2]):
    """Helper function to verify flag builder behavior"""
    all_expected_props = {
        'key': 'test-flag',
        'version': 1,
        'on': True,
        'prerequisites': [],
        'targets': [],
        'contextTargets': [],
        'rules': [],
        'salt': '',
        'variations': [True, False],
        'offVariation': 1,
        'fallthrough': {'variation': 0},
    }
    all_expected_props.update(expected_props)

    td = TestDataV2.data_source()
    flag_builder = builder_actions(td.flag(key='test-flag'))
    built_flag = flag_builder._build(1)
    assert built_flag == all_expected_props, f"did not get expected flag properties for '{desc}' test"


@pytest.mark.parametrize(
    'expected_props,builder_actions',
    [
        pytest.param({}, lambda f: f, id='defaults'),
        pytest.param({}, lambda f: f.boolean_flag(), id='changing default flag to boolean flag has no effect'),
        pytest.param(
            {},
            lambda f: f.variations('a', 'b').boolean_flag(),
            id='non-boolean flag can be changed to boolean flag',
        ),
        pytest.param({'on': False}, lambda f: f.on(False), id='flag can be turned off'),
        pytest.param(
            {},
            lambda f: f.on(False).on(True),
            id='flag can be turned on',
        ),
        pytest.param({'fallthrough': {'variation': 1}}, lambda f: f.variation_for_all(False), id='set false variation for all'),
        pytest.param({'fallthrough': {'variation': 0}}, lambda f: f.variation_for_all(True), id='set true variation for all'),
        pytest.param({'variations': ['a', 'b', 'c'], 'fallthrough': {'variation': 2}}, lambda f: f.variations('a', 'b', 'c').variation_for_all(2), id='set variation index for all'),
        pytest.param({'offVariation': 0}, lambda f: f.off_variation(True), id='set off variation boolean'),
        pytest.param({'variations': ['a', 'b', 'c'], 'offVariation': 2}, lambda f: f.variations('a', 'b', 'c').off_variation(2), id='set off variation index'),
        pytest.param(
            {
                'targets': [
                    {'variation': 0, 'values': ['key1', 'key2']},
                ],
                'contextTargets': [
                    {'contextKind': 'user', 'variation': 0, 'values': []},
                    {'contextKind': 'kind1', 'variation': 0, 'values': ['key3', 'key4']},
                    {'contextKind': 'kind1', 'variation': 1, 'values': ['key5', 'key6']},
                ],
            },
            lambda f: f.variation_for_key('user', 'key1', True)
            .variation_for_key('user', 'key2', True)
            .variation_for_key('kind1', 'key3', True)
            .variation_for_key('kind1', 'key5', False)
            .variation_for_key('kind1', 'key4', True)
            .variation_for_key('kind1', 'key6', False),
            id='set context targets as boolean',
        ),
        pytest.param(
            {
                'variations': ['a', 'b'],
                'targets': [
                    {'variation': 0, 'values': ['key1', 'key2']},
                ],
                'contextTargets': [
                    {'contextKind': 'user', 'variation': 0, 'values': []},
                    {'contextKind': 'kind1', 'variation': 0, 'values': ['key3', 'key4']},
                    {'contextKind': 'kind1', 'variation': 1, 'values': ['key5', 'key6']},
                ],
            },
            lambda f: f.variations('a', 'b')
            .variation_for_key('user', 'key1', 0)
            .variation_for_key('user', 'key2', 0)
            .variation_for_key('kind1', 'key3', 0)
            .variation_for_key('kind1', 'key5', 1)
            .variation_for_key('kind1', 'key4', 0)
            .variation_for_key('kind1', 'key6', 1),
            id='set context targets as variation index',
        ),
        pytest.param(
            {'contextTargets': [{'contextKind': 'kind1', 'variation': 0, 'values': ['key1', 'key2']}, {'contextKind': 'kind1', 'variation': 1, 'values': ['key3']}]},
            lambda f: f.variation_for_key('kind1', 'key1', 0).variation_for_key('kind1', 'key2', 1).variation_for_key('kind1', 'key3', 1).variation_for_key('kind1', 'key2', 0),
            id='replace existing context target key',
        ),
        pytest.param(
            {
                'variations': ['a', 'b'],
                'contextTargets': [
                    {'contextKind': 'kind1', 'variation': 1, 'values': ['key1']},
                ],
            },
            lambda f: f.variations('a', 'b').variation_for_key('kind1', 'key1', 1).variation_for_key('kind1', 'key2', 3),
            id='ignore target for nonexistent variation',
        ),
        pytest.param(
            {'targets': [{'variation': 0, 'values': ['key1']}], 'contextTargets': [{'contextKind': 'user', 'variation': 0, 'values': []}]},
            lambda f: f.variation_for_user('key1', True),
            id='variation_for_user is shortcut for variation_for_key',
        ),
        pytest.param({}, lambda f: f.variation_for_key('kind1', 'key1', 0).clear_targets(), id='clear targets'),
        pytest.param(
            {'rules': [{'variation': 1, 'id': 'rule0', 'clauses': [{'contextKind': 'kind1', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': False}]}]},
            lambda f: f.if_match_context('kind1', 'attr1', 'a', 'b').then_return(1),
            id='if_match_context',
        ),
        pytest.param(
            {'rules': [{'variation': 1, 'id': 'rule0', 'clauses': [{'contextKind': 'kind1', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': True}]}]},
            lambda f: f.if_not_match_context('kind1', 'attr1', 'a', 'b').then_return(1),
            id='if_not_match_context',
        ),
        pytest.param(
            {'rules': [{'variation': 1, 'id': 'rule0', 'clauses': [{'contextKind': 'user', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': False}]}]},
            lambda f: f.if_match('attr1', 'a', 'b').then_return(1),
            id='if_match is shortcut for if_match_context',
        ),
        pytest.param(
            {'rules': [{'variation': 1, 'id': 'rule0', 'clauses': [{'contextKind': 'user', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': True}]}]},
            lambda f: f.if_not_match('attr1', 'a', 'b').then_return(1),
            id='if_not_match is shortcut for if_not_match_context',
        ),
        pytest.param(
            {
                'rules': [
                    {
                        'variation': 1,
                        'id': 'rule0',
                        'clauses': [
                            {'contextKind': 'kind1', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': False},
                            {'contextKind': 'kind1', 'attribute': 'attr2', 'op': 'in', 'values': ['c', 'd'], 'negate': False},
                        ],
                    }
                ]
            },
            lambda f: f.if_match_context('kind1', 'attr1', 'a', 'b').and_match_context('kind1', 'attr2', 'c', 'd').then_return(1),
            id='and_match_context',
        ),
        pytest.param(
            {
                'rules': [
                    {
                        'variation': 1,
                        'id': 'rule0',
                        'clauses': [
                            {'contextKind': 'kind1', 'attribute': 'attr1', 'op': 'in', 'values': ['a', 'b'], 'negate': False},
                            {'contextKind': 'kind1', 'attribute': 'attr2', 'op': 'in', 'values': ['c', 'd'], 'negate': True},
                        ],
                    }
                ]
            },
            lambda f: f.if_match_context('kind1', 'attr1', 'a', 'b').and_not_match_context('kind1', 'attr2', 'c', 'd').then_return(1),
            id='and_not_match_context',
        ),
        pytest.param({}, lambda f: f.if_match_context('kind1', 'attr1', 'a').then_return(1).clear_rules(), id='clear rules'),
    ],
)
def test_flag_configs_parameterized_v2(expected_props: dict, builder_actions: Callable[[FlagBuilderV2], FlagBuilderV2]):
    verify_flag_builder_v2('x', expected_props, builder_actions)


def test_initializer_fetches_flag_data():
    """Test that initializer returns flag data correctly"""
    td = TestDataV2.data_source()
    td.update(td.flag('some-flag').variation_for_all(True))

    initializer = td.build_initializer()
    result = initializer.fetch()

    assert isinstance(result, _Success)
    basis = result.value
    assert len(basis.change_set.changes) == 1

    change = basis.change_set.changes[0]
    assert change.action == ChangeType.PUT
    assert change.kind == ObjectKind.FLAG
    assert change.key == 'some-flag'
    assert change.object['key'] == 'some-flag'
    assert change.object['on'] is True


def test_synchronizer_yields_initial_data():
    """Test that synchronizer yields initial data correctly"""
    td = TestDataV2.data_source()
    td.update(td.flag('initial-flag').variation_for_all(False))

    synchronizer = td.build_synchronizer()

    update_iter = iter(synchronizer.sync())
    initial_update = next(update_iter)

    assert initial_update.state == DataSourceState.VALID
    assert initial_update.change_set is not None
    assert initial_update.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(initial_update.change_set.changes) == 1

    change = initial_update.change_set.changes[0]
    assert change.key == 'initial-flag'

    synchronizer.close()


def test_synchronizer_receives_updates():
    """Test that synchronizer receives flag updates"""
    td = TestDataV2.data_source()
    synchronizer = td.build_synchronizer()

    updates = []
    update_count = 0

    def collect_updates():
        nonlocal update_count
        for update in synchronizer.sync():
            updates.append(update)
            update_count += 1

            if update_count >= 2:
                synchronizer.close()
                break

    # Start the synchronizer in a thread
    sync_thread = threading.Thread(target=collect_updates)
    sync_thread.start()

    # Wait a bit for initial update
    time.sleep(0.1)

    # Update a flag
    td.update(td.flag('updated-flag').variation_for_all(True))

    # Wait for the thread to complete
    sync_thread.join(timeout=5)

    assert len(updates) >= 2

    # First update should be initial (empty)
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL

    # Second update should be the flag change
    assert updates[1].state == DataSourceState.VALID
    assert updates[1].change_set.intent_code == IntentCode.TRANSFER_CHANGES
    assert len(updates[1].change_set.changes) == 1
    assert updates[1].change_set.changes[0].key == 'updated-flag'


def test_multiple_synchronizers_receive_updates():
    """Test that multiple synchronizers receive the same updates"""
    td = TestDataV2.data_source()
    sync1 = td.build_synchronizer()
    sync2 = td.build_synchronizer()

    updates1 = []
    updates2 = []

    def collect_updates_1():
        for update in sync1.sync():
            updates1.append(update)
            if len(updates1) >= 2:
                sync1.close()
                break

    def collect_updates_2():
        for update in sync2.sync():
            updates2.append(update)
            if len(updates2) >= 2:
                sync2.close()
                break

    # Start both synchronizers
    thread1 = threading.Thread(target=collect_updates_1)
    thread2 = threading.Thread(target=collect_updates_2)

    thread1.start()
    thread2.start()

    time.sleep(0.1)  # Let them get initial state

    # Update a flag
    td.update(td.flag('shared-flag').variation_for_all(True))

    thread1.join(timeout=5)
    thread2.join(timeout=5)

    assert len(updates1) >= 2
    assert len(updates2) >= 2

    # Both should receive the same updates
    assert updates1[1].change_set.changes[0].key == 'shared-flag'
    assert updates2[1].change_set.changes[0].key == 'shared-flag'


def test_closed_synchronizer_stops_yielding():
    """Test that closed synchronizer stops yielding updates"""
    td = TestDataV2.data_source()
    synchronizer = td.build_synchronizer()

    updates = []

    # Get initial update then close
    for update in synchronizer.sync():
        updates.append(update)
        synchronizer.close()
        break

    assert len(updates) == 1

    # Further updates should not be received
    td.update(td.flag('post-close-flag').variation_for_all(True))

    # Try to get more updates - should get an error state indicating closure
    additional_updates = []
    for update in synchronizer.sync():
        additional_updates.append(update)
        break

    # Should get exactly one error update indicating the synchronizer is closed
    assert len(additional_updates) == 1
    assert additional_updates[0].state == DataSourceState.OFF
    assert "TestDataV2 source has been closed" in additional_updates[0].error.message


def test_initializer_can_sync():
    """Test that an initializer can call sync() and get initial data"""
    td = TestDataV2.data_source()
    td.update(td.flag('test-flag').variation_for_all(True))

    initializer = td.build_initializer()
    sync_gen = initializer.sync()

    # Should get initial update with data
    initial_update = next(sync_gen)
    assert initial_update.state == DataSourceState.VALID
    assert initial_update.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(initial_update.change_set.changes) == 1
    assert initial_update.change_set.changes[0].key == 'test-flag'


def test_value_for_all():
    """Test value_for_all method creates single-variation flag"""
    td = TestDataV2.data_source()
    flag = td.flag('value-flag').value_for_all('custom-value')
    built_flag = flag._build(1)

    assert built_flag['variations'] == ['custom-value']
    assert built_flag['fallthrough']['variation'] == 0


def test_version_increment():
    """Test that versions increment correctly"""
    td = TestDataV2.data_source()

    flag1 = td.flag('flag1').variation_for_all(True)
    td.update(flag1)

    flag2 = td.flag('flag1').variation_for_all(False)
    td.update(flag2)

    # Get the final flag data
    data = td._make_init_data()
    assert data['flag1']['version'] == 2  # Should have incremented


def test_error_handling_in_fetch():
    """Test error handling in the fetch method"""
    td = TestDataV2.data_source()
    initializer = td.build_initializer()

    # Close the initializer to trigger error condition
    initializer.close()

    result = initializer.fetch()
    assert isinstance(result, _Fail)
    assert "TestDataV2 source has been closed" in result.error
