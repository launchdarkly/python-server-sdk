from typing import Callable

import pytest

from ldclient.client import Context, LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.integrations.test_data import FlagBuilder, TestData
from ldclient.versioned_data_kind import FEATURES

# Test Data + Data Source


def test_makes_valid_datasource():
    td = TestData.data_source()
    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store))

    assert store.all(FEATURES, lambda x: x) == {}


def verify_flag_builder(desc: str, expected_props: dict, builder_actions: Callable[[FlagBuilder], FlagBuilder]):
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

    td = TestData.data_source()
    flag_builder = builder_actions(td.flag(key='test-flag'))
    built_flag = flag_builder._build(1)
    assert built_flag == all_expected_props, "did not get expected flag properties for '%s' test" % desc


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
def test_flag_configs_parameterized(expected_props: dict, builder_actions: Callable[[FlagBuilder], FlagBuilder]):
    verify_flag_builder('x', expected_props, builder_actions)


def test_can_retrieve_flag_from_store():
    td = TestData.data_source()
    td.update(td.flag('some-flag'))

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store))

    assert store.get(FEATURES, 'some-flag') == FEATURES.decode(td.flag('some-flag')._build(1))

    client.close()


def test_updates_to_flags_are_reflected_in_store():
    td = TestData.data_source()

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store))

    td.update(td.flag('some-flag'))

    assert store.get(FEATURES, 'some-flag') == FEATURES.decode(td.flag('some-flag')._build(1))

    client.close()


def test_updates_after_client_close_have_no_affect():
    td = TestData.data_source()

    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store))

    client.close()

    td.update(td.flag('some-flag'))

    assert store.get(FEATURES, 'some-flag') is None


def test_can_handle_multiple_clients():
    td = TestData.data_source()
    flag_builder = td.flag('flag')
    built_flag = flag_builder._build(1)
    td.update(flag_builder)

    store = InMemoryFeatureStore()
    store2 = InMemoryFeatureStore()

    config = Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store)
    client = LDClient(config=config)

    config2 = Config('SDK_KEY', update_processor_class=td, send_events=False, offline=True, feature_store=store2)
    client2 = LDClient(config=config2)

    assert store.get(FEATURES, 'flag') == FEATURES.decode(built_flag)

    assert store2.get(FEATURES, 'flag') == FEATURES.decode(built_flag)

    flag_builder_v2 = td.flag('flag').variation_for_all(False)
    td.update(flag_builder_v2)
    built_flag_v2 = flag_builder_v2._build(2)

    assert store.get(FEATURES, 'flag') == FEATURES.decode(built_flag_v2)

    assert store2.get(FEATURES, 'flag') == FEATURES.decode(built_flag_v2)

    client.close()
    client2.close()


def test_flag_evaluation_with_client():
    td = TestData.data_source()
    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, feature_store=store))

    td.update(td.flag(key='test-flag').fallthrough_variation(False).if_match('firstName', 'Mike').and_not_match('country', 'gb').then_return(True))

    # user1 should satisfy the rule (matching firstname, not matching country)
    user1 = Context.from_dict({'kind': 'user', 'key': 'user1', 'firstName': 'Mike', 'country': 'us'})
    eval1 = client.variation_detail('test-flag', user1, default='default')

    assert eval1.value is True
    assert eval1.variation_index == 0
    assert eval1.reason['kind'] == 'RULE_MATCH'

    # user2 should NOT satisfy the rule (not matching firstname despite not matching country)
    user2 = Context.from_dict({'kind': 'user', 'key': 'user2', 'firstName': 'Joe', 'country': 'us'})
    eval2 = client.variation_detail('test-flag', user2, default='default')

    assert eval2.value is False
    assert eval2.variation_index == 1
    assert eval2.reason['kind'] == 'FALLTHROUGH'


def test_flag_can_evaluate_all_flags():
    td = TestData.data_source()
    store = InMemoryFeatureStore()

    client = LDClient(config=Config('SDK_KEY', update_processor_class=td, send_events=False, feature_store=store))

    td.update(td.flag(key='test-flag').fallthrough_variation(False).if_match('firstName', 'Mike').and_not_match('country', 'gb').then_return(True))

    user1 = Context.from_dict({'kind': 'user', 'key': 'user1', 'firstName': 'Mike', 'country': 'us'})
    flags_state = client.all_flags_state(user1, with_reasons=True)

    assert flags_state.valid

    value = flags_state.get_flag_value('test-flag')
    reason = flags_state.get_flag_reason('test-flag') or {}

    assert value is True
    assert reason.get('kind', None) == 'RULE_MATCH'
