import pytest

from ldclient import Context
from testing.impl.evaluator_util import *


def _segment_matches_user(segment: dict, context: Context) -> bool:
    e = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = e.evaluate(flag, context, event_factory)
    return result.detail.value


def test_explicit_include_user():
    s = {
        "key": "test",
        "included": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_user(s, u) is True

def test_explicit_exclude_user():
    s = {
        "key": "test",
        "excluded": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_user(s, u) is False

def test_explicit_include_has_precedence():
    s = {
        "key": "test",
        "included": [ "foo" ],
        "excluded": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_user(s, u) is True

def test_matching_rule_with_no_weight():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    }
                ]
            }
        ]
    }
    u = Context.builder('foo').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is True

def test_matching_rule_with_none_weight():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    }
                ],
                "weight": None
            }
        ]
    }
    u = Context.builder('foo').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is True

def test_matching_rule_with_full_rollout():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    }
                ],
                "weight": 100000
            }
        ]
    }
    u = Context.builder('foo').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is True

def test_matching_rule_with_zero_rollout():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    }
                ],
                "weight": 0
            }
        ]
    }
    u = Context.builder('foo').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is False

def test_matching_rule_with_multiple_clauses():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    },
                    {
                        "attribute": "name",
                        "op": "in",
                        "values": [ "bob" ]
                    }
                ],
                "weight": 100000
            }
        ]
    }
    u = Context.builder('foo').name('bob').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is True

def test_non_matching_rule_with_multiple_clauses():
    s = {
        "key": "test",
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "email",
                        "op": "in",
                        "values": [ "test@example.com" ]
                    },
                    {
                        "attribute": "name",
                        "op": "in",
                        "values": [ "bill" ]
                    }
                ],
                "weight": 100000
            }
        ]
    }
    u = Context.builder('foo').name('bob').set('email', 'test@example.com').build()
    assert _segment_matches_user(s, u) is False
