"""
Tests for AsyncEvaluator — an async port of Evaluator.

These tests mirror the key scenarios from the sync evaluator test files but use
@pytest.mark.asyncio and async def, and wire async callables into AsyncEvaluatorBuilder.
"""
import pytest

from ldclient import Context
from ldclient.evaluation import BigSegmentsStatus, EvaluationDetail
from ldclient.impl.async_evaluator import AsyncEvaluator, _make_big_segment_ref
from ldclient.impl.events.types import EventFactory, EventInputEvaluation
from ldclient.impl.model import *
from ldclient.testing.builders import *

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

basic_user = Context.create('user-key')
fake_timestamp = 0
event_factory = EventFactory(False, lambda: fake_timestamp)


class AsyncEvaluatorBuilder:
    """Builds an AsyncEvaluator with in-memory flag/segment/big-segment stores."""

    def __init__(self):
        self.__flags = {}
        self.__segments = {}
        self.__big_segments = {}
        self.__big_segments_status = BigSegmentsStatus.HEALTHY

    def build(self) -> AsyncEvaluator:
        return AsyncEvaluator(self._get_flag, self._get_segment, self._get_big_segments_membership)

    def with_flag(self, flag: FeatureFlag) -> 'AsyncEvaluatorBuilder':
        self.__flags[flag.key] = flag
        return self

    def with_unknown_flag(self, key) -> 'AsyncEvaluatorBuilder':
        self.__flags[key] = None
        return self

    def with_segment(self, segment: Segment) -> 'AsyncEvaluatorBuilder':
        self.__segments[segment.key] = segment
        return self

    def with_unknown_segment(self, key) -> 'AsyncEvaluatorBuilder':
        self.__segments[key] = None
        return self

    def with_big_segment_for_key(self, key: str, segment: Segment, included: bool) -> 'AsyncEvaluatorBuilder':
        if key not in self.__big_segments:
            self.__big_segments[key] = {}
        self.__big_segments[key][_make_big_segment_ref(segment)] = included
        return self

    def with_no_big_segments_for_key(self, key: str) -> 'AsyncEvaluatorBuilder':
        self.__big_segments[key] = {}
        return self

    def with_big_segments_status(self, status: str) -> 'AsyncEvaluatorBuilder':
        self.__big_segments_status = status
        return self

    async def _get_flag(self, key: str):
        if key not in self.__flags:
            raise Exception("test made unexpected request for flag '%s'" % key)
        return self.__flags[key]

    async def _get_segment(self, key: str):
        if key not in self.__segments:
            raise Exception("test made unexpected request for segment '%s'" % key)
        return self.__segments[key]

    async def _get_big_segments_membership(self, key: str):
        if key not in self.__big_segments:
            raise Exception("test made unexpected request for big segments for context key '%s'" % key)
        return self.__big_segments[key], self.__big_segments_status


basic_evaluator = AsyncEvaluatorBuilder().build()


def assert_eval_result(result, expected_detail, expected_events):
    assert result.detail == expected_detail
    assert result.events == expected_events


# ---------------------------------------------------------------------------
# Basic flag evaluation (on/off/fallthrough)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_flag_returns_off_variation_if_flag_is_off():
    flag = FlagBuilder('feature').on(False).off_variation(1).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'OFF'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_none_if_flag_is_off_and_off_variation_is_unspecified():
    flag = FlagBuilder('feature').on(False).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'OFF'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_off_variation_is_too_high():
    flag = FlagBuilder('feature').on(False).off_variation(999).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_off_variation_is_negative():
    flag = FlagBuilder('feature').on(False).off_variation(-1).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_fallthrough_variation_is_too_high():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_variation(999).build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_fallthrough_variation_is_negative():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_variation(-1).build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_fallthrough_has_no_variation_or_rollout():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').build()
    user = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_fallthrough_variation():
    flag = FlagBuilder('feature').on(True).variations('a', 'b', 'c').fallthrough_variation(0).build()
    user = Context.create('x')
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


# ---------------------------------------------------------------------------
# Rule matching
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_flag_matches_user_from_rules():
    rule = {'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 0}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(True, 0, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'ruleId': 'id'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_rule_variation_is_too_high():
    rule = {'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': 999}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_error_if_rule_variation_is_negative():
    rule = {'id': 'id', 'clauses': [{'attribute': 'key', 'op': 'in', 'values': ['userkey']}], 'variation': -1}
    flag = make_boolean_flag_with_rules(rule)
    user = Context.create('userkey')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await basic_evaluator.evaluate(flag, user, event_factory), detail, None)


# ---------------------------------------------------------------------------
# Prerequisite evaluation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_flag_returns_off_variation_if_prerequisite_not_found():
    flag = FlagBuilder('feature').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('badfeature', 1).build()
    evaluator = AsyncEvaluatorBuilder().with_unknown_flag('badfeature').build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'badfeature'})
    assert_eval_result(await evaluator.evaluate(flag, user, event_factory), detail, None)


@pytest.mark.asyncio
async def test_flag_returns_off_variation_and_event_if_prerequisite_is_off():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(False).off_variation(1).variations('d', 'e').fallthrough_variation(1).build()
    # note that even though flag1 returns the desired variation, it is still off and therefore not a match
    evaluator = AsyncEvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 1, 'e', None, None, flag, False)]
    assert_eval_result(await evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


@pytest.mark.asyncio
async def test_flag_returns_off_variation_and_event_if_prerequisite_is_not_met():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(0).build()
    evaluator = AsyncEvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 0, 'd', None, None, flag, False)]
    assert_eval_result(await evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


@pytest.mark.asyncio
async def test_flag_returns_fallthrough_and_event_if_prereq_is_met():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(0).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(1).build()
    evaluator = AsyncEvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 1, 'e', None, None, flag, False)]
    assert_eval_result(await evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2, 3, 4])
async def test_prerequisite_cycle_detection(depth: int):
    flag_keys = list("flagkey%d" % i for i in range(depth))
    flags = []
    for i in range(depth):
        flags.append(FlagBuilder(flag_keys[i]).on(True).variations(False, True).off_variation(0).prerequisite(flag_keys[(i + 1) % depth], 0).build())
    evaluator_builder = AsyncEvaluatorBuilder()
    for f in flags:
        evaluator_builder.with_flag(f)
    evaluator = evaluator_builder.build()
    context = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(await evaluator.evaluate(flags[0], context, event_factory), detail, None)


# ---------------------------------------------------------------------------
# Segment matching
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_segment_match_clause_retrieves_segment_from_store():
    segment = SegmentBuilder('segkey').included('foo').build()
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    user = Context.create('foo')
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, user, event_factory)
    assert result.detail.value is True


@pytest.mark.asyncio
async def test_segment_match_clause_falls_through_if_segment_not_found():
    user = Context.create('foo')
    flag = make_boolean_flag_with_clauses(make_clause_matching_segment_key('segkey'))
    evaluator = AsyncEvaluatorBuilder().with_unknown_segment('segkey').build()
    result = await evaluator.evaluate(flag, user, event_factory)
    assert result.detail.value is False


@pytest.mark.asyncio
async def test_explicit_include_user():
    user = Context.create('foo')
    segment = SegmentBuilder('test').included(user.key).build()
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, user, event_factory)
    assert result.detail.value is True


@pytest.mark.asyncio
async def test_explicit_exclude_user():
    user = Context.create('foo')
    segment = SegmentBuilder('test').excluded(user.key).rules(make_segment_rule_matching_context(user)).build()
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, user, event_factory)
    assert result.detail.value is False


@pytest.mark.asyncio
async def test_matching_segment_rule():
    context = Context.create('foo')
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().clauses(make_clause_matching_context(context)).build()).build()
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, context, event_factory)
    assert result.detail.value is True


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2, 3, 4])
async def test_segment_cycle_detection(depth: int):
    segment_keys = list("segmentkey%d" % i for i in range(depth))
    segments = []
    for i in range(depth):
        segments.append(SegmentBuilder(segment_keys[i]).rules(SegmentRuleBuilder().clauses(make_clause_matching_segment_key(segment_keys[(i + 1) % depth])).build()).build())
    evaluator_builder = AsyncEvaluatorBuilder()
    for s in segments:
        evaluator_builder.with_segment(s)
    evaluator = evaluator_builder.build()
    flag = make_boolean_flag_matching_segment(segments[0])
    context = Context.create('x')
    result = await evaluator.evaluate(flag, context, event_factory)
    assert result.detail.value is None
    assert result.detail.reason == {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'}


# ---------------------------------------------------------------------------
# Big segment matching — verifies await is correctly called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_big_segment_with_no_generation_is_not_matched():
    segment = SegmentBuilder('key').version(1).included(basic_user.key).unbounded(True).build()
    # included should be ignored for a big segment (no generation means NOT_CONFIGURED)
    evaluator = AsyncEvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED


@pytest.mark.asyncio
async def test_big_segment_matched_with_include():
    target_key = basic_user.key
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(2).build()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = (
        AsyncEvaluatorBuilder()
        .with_segment(segment)
        .with_big_segment_for_key(target_key, segment, True)
        .build()
    )
    result = await evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


@pytest.mark.asyncio
async def test_big_segment_unmatched_by_exclude():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(2).rules(make_segment_rule_matching_context(basic_user)).build()
    evaluator = (
        AsyncEvaluatorBuilder()
        .with_segment(segment)
        .with_big_segment_for_key(basic_user.key, segment, False)
        .build()
    )
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


@pytest.mark.asyncio
async def test_big_segment_matched_with_rule_when_not_in_membership():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(2).rules(make_segment_rule_matching_context(basic_user)).build()
    evaluator = (
        AsyncEvaluatorBuilder()
        .with_segment(segment)
        .with_no_big_segments_for_key(basic_user.key)
        .build()
    )
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


@pytest.mark.asyncio
async def test_big_segment_status_is_returned():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(1).build()
    evaluator = (
        AsyncEvaluatorBuilder()
        .with_segment(segment)
        .with_no_big_segments_for_key(basic_user.key)
        .with_big_segments_status(BigSegmentsStatus.NOT_CONFIGURED)
        .build()
    )
    flag = make_boolean_flag_matching_segment(segment)
    result = await evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED


@pytest.mark.asyncio
async def test_big_segment_membership_is_cached_per_context_key():
    """Verify that multiple big segments for the same key only call the async closure once."""
    call_count = 0
    target_key = basic_user.key
    segment1 = SegmentBuilder('seg1').version(1).unbounded(True).generation(1).build()
    segment2 = SegmentBuilder('seg2').version(1).unbounded(True).generation(1).build()

    membership = {_make_big_segment_ref(segment1): True}

    async def get_big_segs(key):
        nonlocal call_count
        call_count += 1
        return membership, BigSegmentsStatus.HEALTHY

    evaluator = AsyncEvaluator(
        _raises_on_get_flag,
        _raises_on_get_segment,
        get_big_segs,
    )

    # Build a flag that matches segment1 OR segment2
    flag = make_boolean_flag_with_clauses(
        make_clause_matching_segment_key('seg1', 'seg2')
    )

    # We need segments in the store — use a wrapper that has both
    builder = AsyncEvaluatorBuilder().with_segment(segment1).with_segment(segment2)
    evaluator2 = AsyncEvaluator(
        builder._get_flag,
        builder._get_segment,
        get_big_segs,
    )

    result = await evaluator2.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is True
    # membership should have been fetched exactly once despite two segment checks
    assert call_count == 1


async def _raises_on_get_flag(key):
    raise Exception("unexpected flag lookup: " + key)


async def _raises_on_get_segment(key):
    raise Exception("unexpected segment lookup: " + key)
