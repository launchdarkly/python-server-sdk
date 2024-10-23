from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.types import EventFactory
from ldclient.testing.builders import *

_event_factory_default = EventFactory(False)
_user = Context.create('x')


def make_basic_flag_with_rules(kind, should_track_events):
    rule_builder = FlagRuleBuilder().rollout({'variations': [{'variation': 0, 'weight': 50000}, {'variation': 1, 'weight': 50000}]})
    if kind == 'rulematch':
        rule_builder.track_events(should_track_events)

    flag_builder = FlagBuilder('feature').on(True).fallthrough_variation(0).variations(False, True).rules(rule_builder.build())
    if kind == 'fallthrough':
        flag_builder.track_events_fallthrough(should_track_events)
    return flag_builder.build()


def test_fallthrough_track_event_false():
    flag = make_basic_flag_with_rules('fallthrough', False)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH'})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is False


def test_fallthrough_track_event_true():
    flag = make_basic_flag_with_rules('fallthrough', True)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH'})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is True


def test_fallthrough_track_event_false_with_experiment():
    flag = make_basic_flag_with_rules('fallthrough', False)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH', 'inExperiment': True})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is True


def test_rulematch_track_event_false():
    flag = make_basic_flag_with_rules('rulematch', False)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is False


def test_rulematch_track_event_true():
    flag = make_basic_flag_with_rules('rulematch', True)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is True


def test_rulematch_track_event_false_with_experiment():
    flag = make_basic_flag_with_rules('rulematch', False)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'inExperiment': True})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.track_events is True
