import pytest
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import EvaluationDetail, EvalResult, _bucket_user, evaluate
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


empty_store = InMemoryFeatureStore()


def make_boolean_flag_with_rules(rules):
    return {
        'key': 'feature',
        'on': True,
        'rules': rules,
        'fallthrough': { 'variation': 0 },
        'variations': [ False, True ],
        'salt': ''
    }


def test_flag_returns_off_variation_if_flag_is_off():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'OFF'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = {
        'key': 'feature',
        'on': False,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'OFF'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_off_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 999,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_off_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': -1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

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
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'badfeature'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_off_variation_and_event_if_prerequisite_is_off():
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
        'off': False,
        'offVariation': 1,
        # note that even though it returns the desired variation, it is still off and therefore not a match
        'fallthrough': { 'variation': 0 },
        'variations': ['d', 'e'],
        'version': 2,
        'trackEvents': False
    }
    store.upsert(FEATURES, flag1)
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e',
        'version': 2, 'user': user, 'prereqOf': 'feature0', 'trackEvents': False, 'debugEventsUntilDate': None, 'reason': None}]
    assert evaluate(flag, user, store) == EvalResult(detail, events_should_be)

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
        'version': 2,
        'trackEvents': False
    }
    store.upsert(FEATURES, flag1)
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 0, 'value': 'd',
        'version': 2, 'user': user, 'prereqOf': 'feature0', 'trackEvents': False, 'debugEventsUntilDate': None, 'reason': None}]
    assert evaluate(flag, user, store) == EvalResult(detail, events_should_be)

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
        'version': 2,
        'trackEvents': False
    }
    store.upsert(FEATURES, flag1)
    user = { 'key': 'x' }
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e',
        'version': 2, 'user': user, 'prereqOf': 'feature0', 'trackEvents': False, 'debugEventsUntilDate': None, 'reason': None}]
    assert evaluate(flag, user, store) == EvalResult(detail, events_should_be)

def test_flag_returns_error_if_fallthrough_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': 999},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_fallthrough_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': -1},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_fallthrough_has_no_variation_or_rollout():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_fallthrough_has_rollout_with_no_variations():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'rollout': {'variations': []}},
        'variations': ['a', 'b', 'c'],
        'salt': ''
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

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
    detail = EvaluationDetail('c', 2, {'kind': 'TARGET_MATCH'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_matches_user_from_rules():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 1}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(True, 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'id'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_rule_variation_is_too_high():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 999}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_rule_variation_is_negative():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': -1}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_rule_has_no_variation_or_rollout():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}]}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_flag_returns_error_if_rule_has_rollout_with_no_variations():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}],
        'rollout': {'variations': []} }
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert evaluate(flag, user, empty_store) == EvalResult(detail, [])

def test_user_key_is_coerced_to_string_for_evaluation():
    clause = { 'attribute': 'key', 'op': 'in', 'values': [ '999' ] }
    flag = _make_bool_flag_from_clause(clause)
    user = { 'key': 999 }
    assert evaluate(flag, user, empty_store).detail.value == True

def test_secondary_key_is_coerced_to_string_for_evaluation():
    # We can't really verify that the rollout calculation works correctly, but we can at least
    # make sure it doesn't error out if there's a non-string secondary value (ch35189)
    rule = {
        'id': 'ruleid',
        'clauses': [
            { 'attribute': 'key', 'op': 'in', 'values': [ 'userkey' ] }
        ],
        'rollout': {
            'salt':  '',
            'variations': [ { 'weight': 100000, 'variation': 1 } ]
        }
    }
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey', 'secondary': 999 }
    assert evaluate(flag, user, empty_store).detail.value == True

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

    assert evaluate(flag, user, store).detail.value == True

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

    assert evaluate(flag, user, empty_store).detail.value == False

def test_clause_matches_builtin_attribute():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store).detail.value == True

def test_clause_matches_custom_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob', 'custom': { 'legs': 4 } }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store).detail.value == True

def test_clause_returns_false_for_missing_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store).detail.value == False

def test_clause_can_be_negated():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ],
        'negate': True
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = _make_bool_flag_from_clause(clause)
    assert evaluate(flag, user, empty_store).detail.value == False


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
