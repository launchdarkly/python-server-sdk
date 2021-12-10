import pytest

from ldclient.evaluation import BigSegmentsStatus
from testing.impl.evaluator_util import *


def test_big_segment_with_no_generation_is_not_matched():
    segment = {
        'key': 'test',
        'included': [ basic_user['key'] ],  # included should be ignored for a big segment
        'version': 1,
        'unbounded': True
    }
    evaluator = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED

def test_big_segment_matched_with_include():
    segment = {
        'key': 'test',
        'version': 1,
        'unbounded': True,
        'generation': 2
    }
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_user(basic_user, segment, True).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_matched_with_rule():
    segment = {
        'key': 'test',
        'version': 1,
        'unbounded': True,
        'generation': 2,
        'rules': [
            { 'clauses': [ make_clause_matching_user(basic_user) ] }
        ]
    }
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_user(basic_user).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == True
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_unmatched_by_exclude_regardless_of_rule():
    segment = {
        'key': 'test',
        'version': 1,
        'unbounded': True,
        'generation': 2,
        'rules': [
            { 'clauses': make_clause_matching_user(basic_user) }
        ]
    }
    evaluator = EvaluatorBuilder().with_segment(segment).with_big_segment_for_user(basic_user, segment, False).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.HEALTHY

def test_big_segment_status_is_returned_by_provider():
    segment = {
        'key': 'test',
        'version': 1,
        'unbounded': True,
        'generation': 1
    }
    evaluator = EvaluatorBuilder().with_segment(segment).with_no_big_segments_for_user(basic_user). \
        with_big_segments_status(BigSegmentsStatus.NOT_CONFIGURED).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = evaluator.evaluate(flag, basic_user, event_factory)
    assert result.detail.value == False
    assert result.detail.reason['bigSegmentsStatus'] == BigSegmentsStatus.NOT_CONFIGURED
