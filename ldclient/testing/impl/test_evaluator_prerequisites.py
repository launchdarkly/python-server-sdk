import pytest

from ldclient.client import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *


def test_flag_returns_off_variation_if_prerequisite_not_found():
    flag = FlagBuilder('feature').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('badfeature', 1).build()
    evaluator = EvaluatorBuilder().with_unknown_flag('badfeature').build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'badfeature'})
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, None)


def test_flag_returns_off_variation_and_event_if_prerequisite_is_off():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(False).off_variation(1).variations('d', 'e').fallthrough_variation(1).build()
    # note that even though flag1 returns the desired variation, it is still off and therefore not a match
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 1, 'e', None, None, flag, False)]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


def test_flag_returns_off_variation_and_event_if_prerequisite_is_not_met():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(1).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(0).build()
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('b', 1, {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': 'feature1'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 0, 'd', None, None, flag, False)]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


def test_flag_returns_fallthrough_and_event_if_prereq_is_met_and_there_are_no_rules():
    flag = FlagBuilder('feature0').on(True).off_variation(1).variations('a', 'b', 'c').fallthrough_variation(0).prerequisite('feature1', 1).build()
    flag1 = FlagBuilder('feature1').version(2).on(True).off_variation(1).variations('d', 'e').fallthrough_variation(1).build()
    evaluator = EvaluatorBuilder().with_flag(flag1).build()
    user = Context.create('x')
    detail = EvaluationDetail('a', 0, {'kind': 'FALLTHROUGH'})
    events_should_be = [EventInputEvaluation(0, user, flag1.key, flag1, 1, 'e', None, None, flag, False)]
    assert_eval_result(evaluator.evaluate(flag, user, event_factory), detail, events_should_be)


@pytest.mark.parametrize("depth", [1, 2, 3, 4])
def test_prerequisite_cycle_detection(depth: int):
    flag_keys = list("flagkey%d" % i for i in range(depth))
    flags = []
    for i in range(depth):
        flags.append(FlagBuilder(flag_keys[i]).on(True).variations(False, True).off_variation(0).prerequisite(flag_keys[(i + 1) % depth], 0).build())
    evaluator_builder = EvaluatorBuilder()
    for f in flags:
        evaluator_builder.with_flag(f)
    evaluator = evaluator_builder.build()
    context = Context.create('x')
    detail = EvaluationDetail(None, None, {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'})
    assert_eval_result(evaluator.evaluate(flags[0], context, event_factory), detail, None)
