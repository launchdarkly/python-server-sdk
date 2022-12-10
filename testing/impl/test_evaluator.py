from ldclient.client import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.evaluator import _context_to_user_dict
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
