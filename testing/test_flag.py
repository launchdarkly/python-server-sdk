import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import evaluate
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


def test_flag_returns_off_variation_if_flag_is_off():
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, store) == ('b', [])

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature',
        'on': False,
        'fallthrough': { 'variation': 0 },
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, store) == (None, [])

def test_flag_returns_off_variation_if_prerequisite_not_found():
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature0',
        'on': True,
        'prerequisites': [{'key': 'badfeature', 'variation': 1}],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    assert evaluate(flag, user, store) == ('b', [])

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
    store = InMemoryFeatureStore()
    flag = {
        'key': 'feature0',
        'on': True,
        'targets': [{ 'values': ['whoever', 'userkey'], 'variation': 2 }],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'userkey' }
    assert evaluate(flag, user, store) == ('c', [])

def test_flag_matches_user_from_rules():
    store = InMemoryFeatureStore()
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
    assert evaluate(flag, user, store) == ('c', [])

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
    store = InMemoryFeatureStore()

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

    assert evaluate(flag, user, store) == (False, [])
