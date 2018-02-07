import pytest
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import _bucket_user, evaluate
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


empty_store = InMemoryFeatureStore()


def test_flag_returns_off_variation_if_flag_is_off():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, empty_store) == ('b', [])

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = {
        'key': 'feature',
        'on': False,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, empty_store) == (None, [])

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
    assert evaluate(flag, user, empty_store) == ('b', [])

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
    store.upsert(FEATURES, flag1)
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
    store.upsert(FEATURES, flag1)
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
    assert evaluate(flag, user, empty_store) == ('c', [])

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
    assert evaluate(flag, user, empty_store) == ('c', [])

def test_segment_match_clause_retrieves_segment_from_store():
    store = InMemoryFeatureStore()
    segment = {
        "key": "segkey",
        "included": [ "foo" ],
        "version": 1
    }
    store.upsert(SEGMENTS, segment)

    user = { "key": "foo" }
    flag = {
        "key": "test",
        "variations": [ False, True ],
        "fallthrough": { "variation": 0 },
        "on": True,
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "",
                        "op": "segmentMatch",
                        "values": [ "segkey" ]
                    }
                ],
                "variation": 1
            }
        ]
    }

    assert evaluate(flag, user, store) == (True, [])

def test_segment_match_clause_falls_through_with_no_errors_if_segment_not_found():
    user = { "key": "foo" }
    flag = {
        "key": "test",
        "variations": [ False, True ],
        "fallthrough": { "variation": 0 },
        "on": True,
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "",
                        "op": "segmentMatch",
                        "values": [ "segkey" ]
                    }
                ],
                "variation": 1
            }
        ]
    }

    assert evaluate(flag, user, empty_store) == (False, [])

def test_clause_matches_builtin_attribute():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store) == (True, [])

def test_clause_matches_custom_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob', 'custom': { 'legs': 4 } }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store) == (True, [])

def test_clause_returns_false_for_missing_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store) == (False, [])

def test_clause_can_be_negated():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ],
        'negate': True
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store) == (False, [])


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


def test_bucket_by_user_key():
    user = { u'key': u'userKeyA' }
    bucket = _bucket_user(user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.42157587)

    user = { u'key': u'userKeyB' }
    bucket = _bucket_user(user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.6708485)

    user = { u'key': u'userKeyC' }
    bucket = _bucket_user(user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.10343106)

def test_bucket_by_int_attr():
    feature = { u'key': u'hashKey', u'salt': u'saltyA' }
    user = {
        u'key': u'userKey',
        u'custom': {
            u'intAttr': 33333,
            u'stringAttr': u'33333'
        }
    }
    bucket = _bucket_user(user, 'hashKey', 'saltyA', 'intAttr')
    assert bucket == pytest.approx(0.54771423)
    bucket2 = _bucket_user(user, 'hashKey', 'saltyA', 'stringAttr')
    assert bucket2 == bucket

def test_bucket_by_float_attr_not_allowed():
    feature = { u'key': u'hashKey', u'salt': u'saltyA' }
    user = {
        u'key': u'userKey',
        u'custom': {
            u'floatAttr': 33.5
        }
    }
    bucket = _bucket_user(user, 'hashKey', 'saltyA', 'floatAttr')
    assert bucket == 0.0
