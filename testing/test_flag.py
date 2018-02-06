import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import evaluate


emptyStore = InMemoryFeatureStore()


def test_flag_returns_off_variation_if_flag_is_off():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, emptyStore) == ('b', [])

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = {
        'key': 'feature',
        'on': False,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, emptyStore) == (None, [])

def test_flag_returns_off_variation_if_prerequisite_not_found():
    flag = {
        'key': 'feature0',
        'on': True,
        'prerequisites': [{'key': 'badfeature', 'variation': 1}],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, emptyStore) == ('b', [])

def test_flag_returns_off_variation_and_event_if_prerequisite_is_not_met():
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature0',
        'on': True,
        'prerequisites': [{'key': 'feature1', 'variation': 1}],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c'],
        'version': 1
    }
    flag1 = {
        'key': 'feature1',
        'on': True,
        'fallthrough': { 'variation': 0 },
        'variations': ['d', 'e'],
        'version': 2
    }
    store.upsert('feature1', flag1)
    user = { 'key': 'x' }
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'value': 'd', 'version': 2,
        'user': user, 'prereqOf': 'feature0'}]
    assert evaluate(flag, user, store) == ('b', events_should_be)

def test_flag_returns_fallthrough_and_event_if_prereq_is_met_and_there_are_no_rules():
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature0',
        'on': True,
        'prerequisites': [{ 'key': 'feature1', 'variation': 1 }],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c'],
        'version': 1
    }
    flag1 = {
        'key': 'feature1',
        'on': True,
        'fallthrough': { 'variation': 1 },
        'variations': ['d', 'e'],
        'version': 2
    }
    store.upsert('feature1', flag1)
    user = { 'key': 'x' }
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'value': 'e', 'version': 2,
        'user': user, 'prereqOf': 'feature0'}]
    assert evaluate(flag, user, store) == ('a', events_should_be)

def test_flag_matches_user_from_targets():
    flag = {
        'key': 'feature0',
        'on': True,
        'targets': [{ 'values': ['whoever', 'userkey'], 'variation': 2 }],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'userkey' }
    assert evaluate(flag, user, emptyStore) == ('c', [])

def test_flag_matches_user_from_rules():
    flag = {
        'key': 'feature0',
        'on': True,
        'rules': [
            {
                'clauses': [
                    {
                        'attribute': 'key',
                        'op': 'in',
                        'values': [ 'userkey' ]
                    }
                ],
                'variation': 2
            }
        ],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'userkey' }
    assert evaluate(flag, user, emptyStore) == ('c', [])

def test_clause_matches_builtin_attribute():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, emptyStore) == (True, [])

def test_clause_matches_custom_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob', 'custom': { 'legs': 4 } }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, emptyStore) == (True, [])

def test_clause_returns_false_for_missing_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, emptyStore) == (False, [])

def test_clause_can_be_negated():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ],
        'negate': True
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, emptyStore) == (False, [])


def _make_bool_flag_from_clause(clause):
    return {
        'key': 'feature',
        'on': True,
        'rules': [
            {
                'clauses': [ clause ],
                'variation': 1
            }
        ],
        'fallthrough': { 'variation': 0 },
        'offVariation': 0,
        'variations': [ False, True ]
    }
