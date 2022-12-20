from ldclient.client import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.evaluator import _context_to_user_dict
from testing.builders import *
from testing.impl.evaluator_util import *


def test_flag_returns_off_variation_if_flag_is_off():
    flag = FlagBuilder('feature').on(False).off_variation(1).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = FlagBuilder('feature').on(False).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'OFF'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_too_high():
    flag = FlagBuilder('feature').on(False).off_variation(999).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_off_variation_is_negative():
    flag = FlagBuilder('feature').on(False).off_variation(-1).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_off_variation_if_prerequisite_not_found():
    flag = FlagBuilder('feature').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1) \
        .prerequisite('badfeature', 1).build()
    evaluator = EvaluatorBuilder().with_unknown_flag('badfeature').build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'badfeature'})
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_off_variation_and_event_if_prerequisite_is_off():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1) \
        .prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(False).off_variation(1).variations('d', 'e').fallthrough_variation(1) \
        .build()
    # note that even though flag1 returns the desired variation, it is still off and therefore not a match
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_off_variation_and_event_if_prerequisite_is_not_met():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1) \
        .prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(0) \
        .build()
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 0, 'value': 'd', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_fallthrough_and_event_if_prereq_is_met_and_there_are_no_rules():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(0) \
        .prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(1) \
        .build()
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [{'kind': 'feature', 'key': 'feature1', 'variation': 1, 'value': 'e', 'default': None,
        'version': 2, 'user': _context_to_user_dict(user), 'prereqOf': 'feature0'}]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)

def test_flag_returns_error_if_fallthrough_variation_is_too_high():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_variation(999).build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_variation_is_negative():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_variation(-1).build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_has_no_variation_or_rollout():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(basic_evaluator.evaluate(flag, user, event_factory), detail, None)

def test_flag_returns_error_if_fallthrough_has_rollout_with_no_variations():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_rollout({'variations': []}).build()
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
    segment = SegmentBuilder('segkey').included('foo').build()
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    user = Context.create('foo')
    flag = make_boolean_flag_matching_segment(segment)

    assert evaluator.evaluate(flag, user, event_factory).detail.value == True

def test_segment_match_clause_falls_through_with_no_errors_if_segment_not_found():
    user = Context.create('foo')
    flag = make_boolean_flag_with_clauses(make_clause_matching_segment_key('segkey'))
    evaluator = EvaluatorBuilder().with_unknown_segment('segkey').build()
    
    assert evaluator.evaluate(flag, user, event_factory).detail.value == False
