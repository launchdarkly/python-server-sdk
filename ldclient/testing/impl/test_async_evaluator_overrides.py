"""
Tests for the override marking performed by the async evaluator. These mirror the key
scenarios of the sync evaluator override tests.
"""
import pytest

from ldclient import Context
from ldclient.impl.events.types import EventFactory
from ldclient.testing.builders import *
from ldclient.testing.impl.test_async_evaluator import AsyncEvaluatorBuilder
from ldclient.testing.impl.test_evaluator_overrides import (
    assert_override_affected,
    assert_reason_override_affected,
    make_override_test_flag,
    prereq_records,
    require_prereq_record
)

override_test_context = Context.create('userkey')
event_factory_with_reasons = EventFactory(True, lambda: 0)


@pytest.mark.asyncio
async def test_override_flag_marks_evaluation():
    flag = make_override_test_flag('feature').with_override_marker()
    result = await AsyncEvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert_override_affected(True, result)


@pytest.mark.asyncio
async def test_plain_flag_is_not_marked():
    flag = make_override_test_flag('feature')
    result = await AsyncEvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'FALLTHROUGH'}
    assert_override_affected(False, result)


@pytest.mark.asyncio
async def test_malformed_override_flag_error_result_is_marked():
    flag = FlagBuilder('feature').on(True).fallthrough_variation(99).variations('off', 'on').build().with_override_marker()
    result = await AsyncEvaluatorBuilder().build().evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(True, result)


@pytest.mark.asyncio
async def test_prerequisite_cycle_through_an_override_flag_is_marked():
    prereq = make_override_test_flag('prereq', 'feature').with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = AsyncEvaluatorBuilder().with_flag(flag).with_flag(prereq).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(True, result)
    assert prereq_records(result) == []


@pytest.mark.asyncio
async def test_prerequisite_cycle_below_an_override_flag_keeps_the_flag_marking():
    prereq = make_override_test_flag('prereq', 'feature')
    flag = make_override_test_flag('feature', 'prereq').with_override_marker()
    evaluator = AsyncEvaluatorBuilder().with_flag(flag).with_flag(prereq).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['errorKind'] == 'MALFORMED_FLAG'
    assert_override_affected(True, result)


@pytest.mark.asyncio
async def test_override_prerequisite_marks_prerequisite_record_and_top_level():
    prereq = make_override_test_flag('prereq').with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    evaluator = AsyncEvaluatorBuilder().with_flag(prereq).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(True, result)
    assert_reason_override_affected(True, require_prereq_record(result, 'prereq').reason)


@pytest.mark.asyncio
async def test_override_flag_does_not_mark_unaffected_prerequisite_record():
    prereq = make_override_test_flag('prereq')
    flag = make_override_test_flag('feature', 'prereq').with_override_marker()
    evaluator = AsyncEvaluatorBuilder().with_flag(prereq).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(True, result)
    assert_reason_override_affected(False, require_prereq_record(result, 'prereq').reason)


@pytest.mark.asyncio
async def test_unaffected_sibling_prerequisite_record_stays_unmarked():
    s1 = SegmentBuilder('s1').included(override_test_context.key).build()
    d = make_override_test_flag('d').with_override_marker()
    b = make_override_test_flag('b', 'd')
    c = FlagBuilder('c').on(True).variations('off', 'on').fallthrough_variation(0).rules(
        FlagRuleBuilder().variation(1).clauses(make_clause_matching_segment_key(s1.key)).build()
    ).build()
    a = make_override_test_flag('a', 'b', 'c')
    evaluator = AsyncEvaluatorBuilder().with_flag(b).with_flag(c).with_flag(d).with_segment(s1).build()
    result = await evaluator.evaluate(a, override_test_context, event_factory_with_reasons)
    assert_override_affected(True, result)
    assert len(prereq_records(result)) == 3
    assert_reason_override_affected(True, require_prereq_record(result, 'd').reason)
    assert_reason_override_affected(True, require_prereq_record(result, 'b').reason)
    assert_reason_override_affected(False, require_prereq_record(result, 'c').reason)


@pytest.mark.asyncio
async def test_override_segment_read_without_matching_marks_evaluation():
    segment = SegmentBuilder('segment').included('someone-else').build().with_override_marker()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason['kind'] == 'FALLTHROUGH'
    assert_override_affected(True, result)


@pytest.mark.asyncio
async def test_override_segment_referenced_by_segment_rule_marks_evaluation():
    nested = SegmentBuilder('nested-segment').included(override_test_context.key).build().with_override_marker()
    outer = SegmentBuilder('outer-segment').rules(SegmentRuleBuilder().clauses(make_clause_matching_segment_key(nested.key)).build()).build()
    flag = make_boolean_flag_matching_segment(outer)
    evaluator = AsyncEvaluatorBuilder().with_segment(outer).with_segment(nested).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.value is True
    assert_override_affected(True, result)


@pytest.mark.asyncio
async def test_missing_definitions_do_not_mark_evaluation():
    unrelated = make_override_test_flag('unrelated').with_override_marker()
    flag = make_override_test_flag('feature', 'missing')
    evaluator = AsyncEvaluatorBuilder().with_flag(unrelated).with_unknown_flag('missing').build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert result.detail.reason == {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'missing'}
    assert_override_affected(False, result)

    segment_flag = make_boolean_flag_with_clauses(make_clause_matching_segment_key('missing'))
    evaluator = AsyncEvaluatorBuilder().with_unknown_segment('missing').build()
    result = await evaluator.evaluate(segment_flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(False, result)


@pytest.mark.asyncio
@pytest.mark.parametrize("flag_override,prereq_override", [(False, False), (True, False), (False, True), (True, True)])
async def test_result_override_affected_matches_reason_indicator(flag_override: bool, prereq_override: bool):
    prereq = make_override_test_flag('prereq')
    if prereq_override:
        prereq = prereq.with_override_marker()
    flag = make_override_test_flag('feature', 'prereq')
    if flag_override:
        flag = flag.with_override_marker()
    evaluator = AsyncEvaluatorBuilder().with_flag(prereq).build()
    result = await evaluator.evaluate(flag, override_test_context, event_factory_with_reasons)
    assert_override_affected(flag_override or prereq_override, result)
    assert_reason_override_affected(prereq_override, require_prereq_record(result, 'prereq').reason)
