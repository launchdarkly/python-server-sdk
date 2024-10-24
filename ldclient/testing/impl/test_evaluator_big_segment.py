import pytest

from ldclient.evaluation import BigSegmentsStatus
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *


def test_big_segment_with_no_generation_is_not_matched():
    segment = SegmentBuilder('key').version(1).included(basic_user.key).unbounded(True).build()
    # included should be ignored for a big segment
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED


def test_big_segment_matched_with_include_for_default_kind():
    _test_matched_with_include(False, False)
    _test_matched_with_include(False, True)


def test_big_segment_matched_with_include_for_non_default_kind():
    _test_matched_with_include(True, False)
    _test_matched_with_include(True, True)


def _test_matched_with_include(non_default_kind: bool, multi_kind_context: bool):
    target_key = 'contextkey'
    single_kind_context = Context.create(target_key, 'kind1') if non_default_kind else Context.create(target_key)
    eval_context = Context.create_multi(single_kind_context, Context.create('key2', 'kind2')) if multi_kind_context else single_kind_context

    segment = SegmentBuilder('key').version(1).unbounded(True).unbounded_context_kind('kind1' if non_default_kind else None).generation(2).build()
    flag = make_boolean_flag_matching_segment(segment)
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_key(target_key, segment, True).build()

    result = evaluator.evaluate(flag, eval_context, event_factory)
    assert result.detail.value is True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


def test_big_segment_matched_with_rule():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(2).rules(make_segment_rule_matching_context(basic_user)).build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_key(basic_user.key).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


def test_big_segment_unmatched_by_exclude_regardless_of_rule():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(2).rules(make_segment_rule_matching_context(basic_user)).build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_key(basic_user.key, segment, False).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY


def test_big_segment_status_is_returned_by_provider():
    segment = SegmentBuilder('key').version(1).unbounded(True).generation(1).build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_key(basic_user.key).with_big_segments_status(BigSegmentsStatus.NOT_CONFIGURED).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value is False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED
