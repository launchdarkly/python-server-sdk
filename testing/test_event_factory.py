import pytest
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.event_factory import _EventFactory

_event_factory_default = _EventFactory(False)
_user = { 'key': 'x' }

def make_basic_flag_with_rules(kind, should_track_events):
    rule = {
        'rollout': {
            'variations': [
                { 'variation': 0, 'weight': 50000 },
                { 'variation': 1, 'weight': 50000 }
            ]
        }
    }
    if kind == 'rulematch':
        rule.update({'trackEvents': should_track_events})

    flag = {
        'key': 'feature',
        'on': True,
        'rules': [rule],
        'fallthrough': { 'variation': 0 },
        'variations': [ False, True ],
        'salt': ''
    }
    if kind == 'fallthrough':
        flag.update({'trackEventsFallthrough': should_track_events})
    return flag

def test_fallthrough_track_event_false():
    flag = make_basic_flag_with_rules('fallthrough', False)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH'})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.get('trackEvents') is None

def test_fallthrough_track_event_true():
    flag = make_basic_flag_with_rules('fallthrough', True)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH'})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval['trackEvents'] == True

def test_fallthrough_track_event_false_with_experiment():
    flag = make_basic_flag_with_rules('fallthrough', False)
    detail = EvaluationDetail('b', 1, {'kind': 'FALLTHROUGH', 'inExperiment': True})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval['trackEvents'] == True

def test_rulematch_track_event_false():
    flag = make_basic_flag_with_rules('rulematch', False)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval.get('trackEvents') is None

def test_rulematch_track_event_true():
    flag = make_basic_flag_with_rules('rulematch', True)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval['trackEvents'] == True

def test_rulematch_track_event_false_with_experiment():
    flag = make_basic_flag_with_rules('rulematch', False)
    detail = EvaluationDetail('b', 1, {'kind': 'RULE_MATCH', 'ruleIndex': 0, 'inExperiment': True})

    eval = _event_factory_default.new_eval_event(flag, _user, detail, 'b', None)
    assert eval['trackEvents'] == True
