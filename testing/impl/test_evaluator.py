import math
import pytest
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.evaluator import _bucket_user, _variation_index_for_user
from testing.impl.evaluator_util import *


def assert_eval_result(result, expected_detail, expected_events):
    assert result.detail == expected_detail
    assert result.events == expected_events


def test_flag_returns_off_variation_if_flag_is_off():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = {
        'key': 'feature',
        'on': False,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 999,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': -1,
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_off_variation_if_prerequisite_not_found():
    flag = {
        'key': 'feature0',
        'on': True,
        'prerequisites': [{'key': 'badfeature', 'variation': 1}],
        'fallthrough': { 'variation': 0 },
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    evaluator = EvaluatorBuilder().with_unknown_flag('badfeature').build()
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'badfeature'})
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_off_variation_and_event_if_prerequisite_is_off():
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
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': user, 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_off_variation_and_event_if_prerequisite_is_not_met():
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
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = { 'key': 'x' }
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 0, 'value': 'd', 'default': None,
        'version': 2, 'user': user, 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_fallthrough_and_event_if_prereq_is_met_and_there_are_no_rules():
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
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = { 'key': 'x' }
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': user, 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_error_if_fallthrough_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': 999},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': -1},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_has_no_variation_or_rollout():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {},
        'variations': ['a', 'b', 'c']
    }
    user = { 'key': 'x' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

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
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

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
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_matches_user_from_rules():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 1}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(True, 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'id'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_variation_is_too_high():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 999}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_variation_is_negative():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': -1}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_has_no_variation_or_rollout():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}]}
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_has_rollout_with_no_variations():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}],
        'rollout': {'variations': []} }
    flag = make_boolean_flag_with_rules([rule])
    user = { 'key': 'userkey' }
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_user_key_is_coerced_to_string_for_evaluation():
    clause = { 'attribute': 'key', 'op': 'in', 'values': [ '999' ] }
    flag = make_boolean_flag_with_clause(clause)
    user = { 'key': 999 }
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == True

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
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == True

def test_segment_match_clause_retrieves_segment_from_store():
    segment = {
        "key": "segkey",
        "included": [ "foo" ],
        "version": 1
    }
    evaluator = EvaluatorBuilder().with_segment(segment).build()

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

    assert evaluator.evaluate(flag, user, event_factory).detail.value == True

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
    evaluator = EvaluatorBuilder().with_unknown_segment('segkey').build()
    
    assert evaluator.evaluate(flag, user, event_factory).detail.value == False

def test_clause_matches_builtin_attribute():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = make_boolean_flag_with_clause(clause)
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == True

def test_clause_matches_custom_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob', 'custom': { 'legs': 4 } }
    flag = make_boolean_flag_with_clause(clause)
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == True

def test_clause_returns_false_for_missing_attribute():
    clause = {
        'attribute': 'legs',
        'op': 'in',
        'values': [ 4 ]
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = make_boolean_flag_with_clause(clause)
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == False

def test_clause_can_be_negated():
    clause = {
        'attribute': 'name',
        'op': 'in',
        'values': [ 'Bob' ],
        'negate': True
    }
    user = { 'key': 'x', 'name': 'Bob' }
    flag = make_boolean_flag_with_clause(clause)
    assert basic_evaluator.evaluate(flag, user, event_factory).detail.value == False

def test_variation_index_is_returned_for_bucket():
    user = { 'key': 'userkey' }
    flag = { 'key': 'flagkey', 'salt': 'salt' }

    # First verify that with our test inputs, the bucket value will be greater than zero and less than 100000,
    # so we can construct a rollout whose second bucket just barely contains that value
    bucket_value = math.trunc(_bucket_user(None, user, flag['key'], flag['salt'], 'key') * 100000)
    assert bucket_value > 0 and bucket_value < 100000
    
    bad_variation_a = 0
    matched_variation = 1
    bad_variation_b = 2
    rule = {
        'rollout': {
            'variations': [
                { 'variation': bad_variation_a, 'weight': bucket_value }, # end of bucket range is not inclusive, so it will *not* match the target value
                { 'variation': matched_variation, 'weight': 1 }, # size of this bucket is 1, so it only matches that specific value
                { 'variation': bad_variation_b, 'weight': 100000 - (bucket_value + 1) }
            ]
        }
    }
    result_variation = _variation_index_for_user(flag, rule, user)
    assert result_variation == (matched_variation, False)

def test_last_bucket_is_used_if_bucket_value_equals_total_weight():
    user = { 'key': 'userkey' }
    flag = { 'key': 'flagkey', 'salt': 'salt' }

    # We'll construct a list of variations that stops right at the target bucket value
    bucket_value = math.trunc(_bucket_user(None, user, flag['key'], flag['salt'], 'key') * 100000)
    
    rule = {
        'rollout': {
            'variations': [
                { 'variation': 0, 'weight': bucket_value }
            ]
        }
    }
    result_variation = _variation_index_for_user(flag, rule, user)
    assert result_variation == (0, False)
    
def test_bucket_by_user_key():
    user = { u'key': u'userKeyA' }
    bucket = _bucket_user(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.42157587)

    user = { u'key': u'userKeyB' }
    bucket = _bucket_user(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.6708485)

    user = { u'key': u'userKeyC' }
    bucket = _bucket_user(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.10343106)

def test_bucket_by_user_key_with_seed():
    seed = 61
    user = { u'key': u'userKeyA' }
    point = _bucket_user(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.09801207)

    user = { u'key': u'userKeyB' }
    point = _bucket_user(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.14483777)

    user = { u'key': u'userKeyC' }
    point = _bucket_user(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.9242641)

def test_bucket_by_int_attr():
    user = {
        u'key': u'userKey',
        u'custom': {
            u'intAttr': 33333,
            u'stringAttr': u'33333'
        }
    }
    bucket = _bucket_user(None, user, 'hashKey', 'saltyA', 'intAttr')
    assert bucket == pytest.approx(0.54771423)
    bucket2 = _bucket_user(None, user, 'hashKey', 'saltyA', 'stringAttr')
    assert bucket2 == bucket

def test_bucket_by_float_attr_not_allowed():
    user = {
        u'key': u'userKey',
        u'custom': {
            u'floatAttr': 33.5
        }
    }
    bucket = _bucket_user(None, user, 'hashKey', 'saltyA', 'floatAttr')
    assert bucket == 0.0

def test_seed_independent_of_salt_and_hashKey():
    seed = 61
    user = { u'key': u'userKeyA' }
    point1 = _bucket_user(seed, user, 'hashKey', 'saltyA', 'key')
    point2 = _bucket_user(seed, user, 'hashKey', 'saltyB', 'key')
    point3 = _bucket_user(seed, user, 'hashKey2', 'saltyA', 'key')

    assert point1 == point2
    assert point2 == point3

def test_seed_changes_hash_evaluation():
    seed1 = 61
    user = { u'key': u'userKeyA' }
    point1 = _bucket_user(seed1, user, 'hashKey', 'saltyA', 'key')
    seed2 = 62
    point2 = _bucket_user(seed2, user, 'hashKey', 'saltyB', 'key')

    assert point1 != point2
