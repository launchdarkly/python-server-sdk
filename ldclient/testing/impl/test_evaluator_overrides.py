"""
Tests for the override marking performed by the evaluator. An evaluation is override-affected
when any flag or segment definition it reads carries the override marker. The marking
propagates upward from prerequisites to the flags that depend on them and never sideways or
downward.
"""
from typing import List, Optional

import pytest

from ldclient import Context
from ldclient.impl.evaluator_common import EvalResult
from ldclient.impl.events.types import EventFactory, EventInputEvaluation
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import EvaluatorBuilder

override_test_context = Context.create('userkey')

# Events carry their reasons, so a prerequisite record's marking is visible on its reason.
event_factory_with_reasons = EventFactory(True, lambda: 0)


def assert_override_affected(expected: bool, result: EvalResult) -> None:
    """Checks the reason indicator and the result scalar together. Both report the same marking."""
    assert result.override_affected is expected, "result.override_affected"
    assert_reason_override_affected(expected, result.detail.reason)


def assert_reason_override_affected(expected: bool, reason: Optional[dict]) -> None:
    assert reason is not None
    if expected:
        assert reason.get('overrideAffected') is True, "reason indicator: %s" % reason
    else:
        assert 'overrideAffected' not in reason, "reason indicator: %s" % reason


def make_override_test_flag(key: str, *prereq_keys: str) -> FeatureFlag:
    """A flag that is on and serves variation 1 ("on") by fallthrough, with prerequisites that must each serve variation 1."""
    builder = FlagBuilder(key).on(True).fallthrough_variation(1).off_variation(0).variations('off', 'on')
    for prereq_key in prereq_keys:
        builder.prerequisite(prereq_key, 1)
    return builder.build()


def prereq_records(result: EvalResult) -> List[EventInputEvaluation]:
    return result.events or []


def require_prereq_record(result: EvalResult, prereq_key: str) -> EventInputEvaluation:
    for event in prereq_records(result):
        if event.flag is not None and event.flag.key == prereq_key:
            return event
    raise AssertionError("no record for prerequisite %s" % prereq_key)


def test_override_flag_marks_evaluation_when_off():
    flag = FlagBuilder('feature').on(False).off_variation(0).variations('off', 'on').build().with_override_marker()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'OFF'
    assert result.detail.value == 'off'
    assert_override_affected(True, result)


def test_override_flag_marks_evaluation_on_fallthrough():
    flag = make_override_test_flag('feature').with_override_marker()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert result.detail.value == 'on'
    assert_override_affected(True, result)


def test_override_flag_marks_evaluation_on_rule_match():
    rule = FlagRuleBuilder().id('rule-id').variation(1).clauses(make_clause_matching_context(override_test_context)).build()
    flag = FlagBuilder('feature').on(True).fallthrough_variation(0).variations('off', 'on').rules(rule).build().with_override_marker()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'RULE_MATCH'
    assert result.detail.reason['ruleId'] == 'rule-id'
    assert result.detail.value == 'on'
    assert_override_affected(True, result)


def test_plain_flag_alone_is_not_marked():
    flag = FlagBuilder('feature').on(False).off_variation(0).variations('off', 'on').build()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'OFF'}
    assert_override_affected(False, result)


def test_plain_flag_with_prerequisite_and_segment_is_not_marked():
    segment = SegmentBuilder('segment').included(override_test_context.key).build()
    prereq = FlagBuilder('prereq').on(True).variations('off', 'on').fallthrough_variation(0).rules(
        FlagRuleBuilder().variation(1).clauses(make_clause_matching_segment_key(segment.key)).build()
    ).build()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = EvaluatorBuilder().with_flag(prereq).with_segment(segment).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'FALLTHROUGH'}
    assert result.detail.value == 'on'
    assert_override_affected(False, result)
    assert_reason_override_affected(False, require_prereq_record(result, 'prereq').reason)


def test_malformed_override_flag_error_result_is_marked():
    flag = FlagBuilder('feature').on(True).fallthrough_variation(99).variations('off', 'on').build().with_override_marker()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'ERROR'
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert result.detail.value is None
    assert_override_affected(True, result)


def test_prerequisite_cycle_through_an_override_flag_is_marked():
    # feature -> prereq -> feature; only prereq is an override
    prereq = make_override_test_flag('prereq', 'feature').with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = EvaluatorBuilder().with_flag(flag).with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'ERROR'
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(True, result)
    assert prereq_records(result) == []


def test_prerequisite_cycle_below_an_override_flag_keeps_the_flag_marking():
    # feature -> prereq -> feature; only feature is an override. The failure happens inside the
    # plain prerequisite's subtree, and the top-level marking from the flag's own read survives it.
    prereq = make_override_test_flag('prereq', 'feature')
    flag = make_override_test_flag('feature', 'prereq').with_override_marker()
    evaluator = EvaluatorBuilder().with_flag(flag).with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(True, result)


def test_prerequisite_cycle_through_plain_flags_is_not_marked():
    prereq = make_override_test_flag('prereq', 'feature')
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = EvaluatorBuilder().with_flag(flag).with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(False, result)


def test_override_prerequisite_marks_prerequisite_record_and_top_level():
    prereq = make_override_test_flag('prereq').with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = EvaluatorBuilder().with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert_override_affected(True, result)
    record = require_prereq_record(result, 'prereq')
    assert record.reason is not None
    assert record.reason['kind'] == 'FALLTHROUGH'
    assert_reason_override_affected(True, record.reason)


def test_override_flag_does_not_mark_unaffected_prerequisite_record():
    # The marking propagates upward only. The top-level flag's own marker does not leak into
    # the record of a prerequisite whose subtree read no override definition.
    prereq = make_override_test_flag('prereq')
    flag = make_override_test_flag('feature', 'prereq').with_override_marker()
    evaluator = EvaluatorBuilder().with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(True, result)
    assert_reason_override_affected(False, require_prereq_record(result, 'prereq').reason)


def test_override_prerequisite_at_depth_two_marks_all_affected_scopes():
    prereq2 = make_override_test_flag('prereq2').with_override_marker()
    prereq1 = make_override_test_flag('prereq1', 'prereq2')
    flag = make_override_test_flag('feature', 'prereq1')
    evaluator = EvaluatorBuilder().with_flag(prereq1).with_flag(prereq2).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert_override_affected(True, result)

    # The nested record is produced first, during the evaluation of prereq1.
    records = prereq_records(result)
    assert [r.flag.key for r in records] == ['prereq2', 'prereq1']
    assert_reason_override_affected(True, records[0].reason)
    assert_reason_override_affected(True, records[1].reason)


def test_unaffected_sibling_prerequisite_record_stays_unmarked():
    # Flag a has prerequisites b and c. Only d, a prerequisite of b, is an override. The marking
    # reaches a, b, and d. It does not reach the sibling c, and the plain segments s1 and s2
    # mark nothing.
    s1 = SegmentBuilder('s1').included(override_test_context.key).build()
    s2 = SegmentBuilder('s2').included(override_test_context.key).build()
    d = make_override_test_flag('d').with_override_marker()
    b = make_override_test_flag('b', 'd')
    c = FlagBuilder('c').on(True).variations('off', 'on').fallthrough_variation(0).rules(
        FlagRuleBuilder().variation(1).clauses(make_clause_matching_segment_key(s1.key)).build()
    ).build()
    a = FlagBuilder('a').on(True).fallthrough_variation(0).off_variation(0).prerequisite('b', 1).prerequisite('c', 1).rules(
        FlagRuleBuilder().id('rule-s2').variation(1).clauses(make_clause_matching_segment_key(s2.key)).build()
    ).variations('off', 'on').build()

    evaluator = EvaluatorBuilder().with_flag(b).with_flag(c).with_flag(d).with_segment(s1).with_segment(s2).build()
    result = evaluator.evaluate(a, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'RULE_MATCH'
    assert result.detail.value == 'on'
    assert_override_affected(True, result)

    assert len(prereq_records(result)) == 3
    assert_reason_override_affected(True, require_prereq_record(result, 'd').reason)
    assert_reason_override_affected(True, require_prereq_record(result, 'b').reason)
    assert_reason_override_affected(False, require_prereq_record(result, 'c').reason)


def test_override_segment_referenced_by_flag_rule_marks_evaluation():
    segment = SegmentBuilder('segment').included(override_test_context.key).build().with_override_marker()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'RULE_MATCH'
    assert result.detail.value is True
    assert_override_affected(True, result)


def test_override_segment_read_without_matching_marks_evaluation():
    # A read is enough to mark the evaluation. The segment does not need to match.
    segment = SegmentBuilder('segment').included('someone-else').build().with_override_marker()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert result.detail.value is False
    assert_override_affected(True, result)


def test_override_segment_read_through_negated_clause_marks_evaluation():
    segment = SegmentBuilder('segment').included('someone-else').build().with_override_marker()
    flag = make_boolean_flag_with_clauses(negate_clause(make_clause_matching_segment_key(segment.key)))
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'RULE_MATCH'
    assert result.detail.value is True
    assert_override_affected(True, result)


def test_override_segment_referenced_by_segment_rule_marks_evaluation():
    # The outer segment is plain. A rule of the outer segment reads a nested override segment.
    nested = SegmentBuilder('nested-segment').included(override_test_context.key).build().with_override_marker()
    outer = SegmentBuilder('outer-segment').rules(SegmentRuleBuilder().clauses(make_clause_matching_segment_key(nested.key)).build()).build()
    flag = make_boolean_flag_matching_segment(outer)
    evaluator = EvaluatorBuilder().with_segment(outer).with_segment(nested).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'RULE_MATCH'
    assert result.detail.value is True
    assert_override_affected(True, result)


def test_override_segment_referenced_by_prerequisite_marks_prerequisite_record_and_top_level():
    segment = SegmentBuilder('segment').included(override_test_context.key).build().with_override_marker()
    prereq = FlagBuilder('prereq').on(True).variations('off', 'on').fallthrough_variation(0).rules(
        FlagRuleBuilder().variation(1).clauses(make_clause_matching_segment_key(segment.key)).build()
    ).build()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = EvaluatorBuilder().with_flag(prereq).with_segment(segment).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert_override_affected(True, result)
    assert_reason_override_affected(True, require_prereq_record(result, 'prereq').reason)


def test_missing_prerequisite_does_not_mark_evaluation():
    # A definition that cannot be resolved contributes nothing, because nothing was read. The
    # store holds an unrelated override definition to show that only reads count.
    unrelated = make_override_test_flag('unrelated').with_override_marker()
    flag = make_override_test_flag('feature', 'missing')
    evaluator = EvaluatorBuilder().with_flag(unrelated).with_unknown_flag('missing').build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'missing'}
    assert result.detail.value == 'off'
    assert_override_affected(False, result)
    assert prereq_records(result) == []


def test_missing_segment_does_not_mark_evaluation():
    unrelated = SegmentBuilder('unrelated').included(override_test_context.key).build().with_override_marker()
    flag = make_boolean_flag_with_clauses(make_clause_matching_segment_key('missing'))
    evaluator = EvaluatorBuilder().with_segment(unrelated).with_unknown_segment('missing').build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'FALLTHROUGH'}
    assert result.detail.value is False
    assert_override_affected(False, result)


@pytest.mark.parametrize("flag_override,prereq_override", [(False, False), (True, False), (False, True), (True, True)])
def test_result_override_affected_matches_reason_indicator(flag_override: bool, prereq_override: bool):
    prereq = make_override_test_flag('prereq')
    if prereq_override:
        prereq = prereq.with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    if flag_override:
        flag = flag.with_override_marker()
    evaluator = EvaluatorBuilder().with_flag(prereq).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(flag_override or prereq_override, result)
    assert_reason_override_affected(prereq_override, require_prereq_record(result, 'prereq').reason)


def test_marking_does_not_disturb_other_reason_properties():
    segment = SegmentBuilder('segment').unbounded(True).generation(1).build().with_override_marker()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_key(override_test_context.key).build()
    result = evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'FALLTHROUGH', 'bigSegmentsStatus': 'HEALTHY', 'overrideAffected': True}


def test_reason_indicator_is_absent_when_events_do_not_carry_reasons():
    # The marking does not depend on the event factory. The reason on the result is marked either way.
    flag = make_override_test_flag('feature').with_override_marker()
    result = EvaluatorBuilder().build().evaluate(flag, override_test_context, EventFactory(False, lambda: 0))
    assert_override_affected(True, result)
