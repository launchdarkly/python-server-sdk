import math
import pytest
from ldclient.client import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.evaluator import _bucket_context, _context_to_user_dict, _variation_index_for_context
from testing.impl.evaluator_util import *


def test_flag_returns_off_variation_if_flag_is_off():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 1,
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = {
        'key': 'feature',
        'on': False,
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': 999,
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': False,
        'offVariation': -1,
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
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
    user = Context.create('x')
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
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
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
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 0, 'value': 'd', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
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
    user = Context.create('x')
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_error_if_fallthrough_variation_is_too_high():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': 999},
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_variation_is_negative():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {'variation': -1},
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_has_no_variation_or_rollout():
    flag = {
        'key': 'feature',
        'on': True,
        'fallthrough': {},
        'variations': ['a', 'b', 'c']
    }
    user = Context.create('x')
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
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_matches_user_from_rules():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 0}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(True, 0, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'id'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_variation_is_too_high():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 999}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_variation_is_negative():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': -1}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_has_no_variation_or_rollout():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}]}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_rule_has_rollout_with_no_variations():
    rule = { 'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}],
        'rollout': {'variations': []} }
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_segment_match_clause_retrieves_segment_from_store():
    segment = {
        "key": "segkey",
        "included": [ "foo" ],
        "version": 1
    }
    evaluator = EvaluatorBuilder().with_segment(segment).build()

    user = Context.create('foo')
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
    user = Context.create('foo')
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

def test_variation_index_is_returned_for_bucket():
    user = Context.create('userkey')
    flag = { 'key': 'flagkey', 'salt': 'salt' }

    # First verify that with our test inputs, the bucket value will be greater than zero and less than 100000,
    # so we can construct a rollout whose second bucket just barely contains that value
    bucket_value = math.trunc(_bucket_context(None, user, flag['key'], flag['salt'], 'key') * 100000)
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
    result_variation = _variation_index_for_context(flag, rule, user)
    assert result_variation == (matched_variation, False)

def test_last_bucket_is_used_if_bucket_value_equals_total_weight():
    user = Context.create('userkey')
    flag = { 'key': 'flagkey', 'salt': 'salt' }

    # We'll construct a list of variations that stops right at the target bucket value
    bucket_value = math.trunc(_bucket_context(None, user, flag['key'], flag['salt'], 'key') * 100000)
    
    rule = {
        'rollout': {
            'variations': [
                { 'variation': 0, 'weight': bucket_value }
            ]
        }
    }
    result_variation = _variation_index_for_context(flag, rule, user)
    assert result_variation == (0, False)
    
def test_bucket_by_user_key():
    user = Context.create('userKeyA')
    bucket = _bucket_context(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.42157587)

    user = Context.create('userKeyB')
    bucket = _bucket_context(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.6708485)

    user = Context.create('userKeyC')
    bucket = _bucket_context(None, user, 'hashKey', 'saltyA', 'key')
    assert bucket == pytest.approx(0.10343106)

def test_bucket_by_user_key_with_seed():
    seed = 61
    user = Context.create('userKeyA')
    point = _bucket_context(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.09801207)

    user = Context.create('userKeyB')
    point = _bucket_context(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.14483777)

    user = Context.create('userKeyC')
    point = _bucket_context(seed, user, 'hashKey', 'saltyA', 'key')
    assert point == pytest.approx(0.9242641)

def test_bucket_by_int_attr():
    user = Context.builder('userKey').set('intAttr', 33333).set('stringAttr', '33333').build()
    bucket = _bucket_context(None, user, 'hashKey', 'saltyA', 'intAttr')
    assert bucket == pytest.approx(0.54771423)
    bucket2 = _bucket_context(None, user, 'hashKey', 'saltyA', 'stringAttr')
    assert bucket2 == bucket

def test_bucket_by_float_attr_not_allowed():
    user = Context.builder('userKey').set('floatAttr', 33.5).build()
    bucket = _bucket_context(None, user, 'hashKey', 'saltyA', 'floatAttr')
    assert bucket == 0.0

def test_seed_independent_of_salt_and_hashKey():
    seed = 61
    user = Context.create('userKeyA')
    point1 = _bucket_context(seed, user, 'hashKey', 'saltyA', 'key')
    point2 = _bucket_context(seed, user, 'hashKey', 'saltyB', 'key')
    point3 = _bucket_context(seed, user, 'hashKey2', 'saltyA', 'key')

    assert point1 == point2
    assert point2 == point3

def test_seed_changes_hash_evaluation():
    seed1 = 61
    user = Context.create('userKeyA')
    point1 = _bucket_context(seed1, user, 'hashKey', 'saltyA', 'key')
    seed2 = 62
    point2 = _bucket_context(seed2, user, 'hashKey', 'saltyB', 'key')

    assert point1 != point2
