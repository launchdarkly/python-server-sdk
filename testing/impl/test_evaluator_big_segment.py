import pytest

from ldclient.evaluation import BigSegmentsStatus
from testing.builders import *
from testing.impl.evaluator_util import *


def test_big_segment_with_no_generation_is_not_matched():
    segment = SegmentBuilder('key').version(1) \
        .included(basic_user.key) \
        .unbounded(True) \
        .build()
    # included should be ignored for a big segment
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED

def test_big_segment_matched_with_include():
    segment = SegmentBuilder('key').version(1) \
        .unbounded(True) \
        .generation(2) \
        .build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_user(basic_user, segment, True).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_matched_with_rule():
    segment = SegmentBuilder('key').version(1) \
        .unbounded(True) \
        .generation(2) \
        .rules(
            make_segment_rule_matching_context(basic_user)
        ) \
        .build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_user(basic_user).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_unmatched_by_exclude_regardless_of_rule():
    segment = SegmentBuilder('key').version(1) \
        .unbounded(True) \
        .generation(2) \
        .rules(
            make_segment_rule_matching_context(basic_user)
        ) \
        .build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_user(basic_user, segment, False).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_status_is_returned_by_provider():
    segment = SegmentBuilder('key').version(1) \
        .unbounded(True) \
        .generation(1) \
        .build()
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_user(basic_user). \
        with_big_segments_status(BigSegmentsStatus.NOT_CONFIGURED).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED
