from typing import Any, Optional, Tuple, Union

from ldclient import Context
from ldclient.evaluation import BigSegmentsStatus
from ldclient.impl.evaluator import Evaluator, _make_big_segment_ref
from ldclient.impl.events.types import EventFactory
from ldclient.impl.model import *
from ldclient.testing.builders import *

basic_user = Context.create('user-key')
fake_timestamp = 0
event_factory = EventFactory(False, lambda: fake_timestamp)


class EvaluatorBuilder:
    def __init__(self):
        self.__flags = {}
        self.__segments = {}
        self.__big_segments = {}
        self.__big_segments_status = BigSegmentsStatus.HEALTHY

    def build(self) -> Evaluator:
        return Evaluator(self._get_flag, self._get_segment, self._get_big_segments_membership)

    def with_flag(self, flag: FeatureFlag) -> 'EvaluatorBuilder':
        self.__flags[flag.key] = flag
        return self

    def with_unknown_flag(self, key) -> 'EvaluatorBuilder':
        self.__flags[key] = None
        return self

    def with_segment(self, segment: Segment) -> 'EvaluatorBuilder':
        self.__segments[segment.key] = segment
        return self

    def with_unknown_segment(self, key) -> 'EvaluatorBuilder':
        self.__segments[key] = None
        return self

    def with_big_segment_for_key(self, key: str, segment: Segment, included: bool) -> 'EvaluatorBuilder':
        if key not in self.__big_segments:
            self.__big_segments[key] = {}
        self.__big_segments[key][_make_big_segment_ref(segment)] = included
        return self

    def with_no_big_segments_for_key(self, key: str) -> 'EvaluatorBuilder':
        self.__big_segments[key] = {}
        return self

    def with_big_segments_status(self, status: str) -> 'EvaluatorBuilder':
        self.__big_segments_status = status
        return self

    def _get_flag(self, key: str) -> Optional[FeatureFlag]:
        if key not in self.__flags:
            raise Exception("test made unexpected request for flag '%s'" % key)
        return self.__flags[key]

    def _get_segment(self, key: str) -> Optional[Segment]:
        if key not in self.__segments:
            raise Exception("test made unexpected request for segment '%s'" % key)
        return self.__segments[key]

    def _get_big_segments_membership(self, key: str) -> Tuple[Optional[dict], str]:
        if key not in self.__big_segments:
            raise Exception("test made unexpected request for big segments for context key '%s'" % key)
        return self.__big_segments[key], self.__big_segments_status


basic_evaluator = EvaluatorBuilder().build()


def assert_eval_result(result, expected_detail, expected_events):
    assert result.detail == expected_detail
    assert result.events == expected_events


def assert_match(evaluator: Evaluator, flag: FeatureFlag, context: Context, expect_value: Any):
    result = evaluator.evaluate(flag, context, event_factory)
    assert result.detail.value == expect_value
