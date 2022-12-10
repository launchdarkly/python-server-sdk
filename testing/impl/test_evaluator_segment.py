import pytest

from ldclient import Context
from ldclient.impl.evaluator import _bucket_context
from testing.builders import *
from testing.impl.evaluator_util import *


def _segment_matches_context(segment: dict, context: Context) -> bool:
    e = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = e.evaluate(flag, context, event_factory)
    return result.detail.value

def verify_rollout(
    eval_context: Context,
    match_context: Context,
    expected_bucket_value: int,
    segment_key: str,
    salt: str,
    bucket_by: Optional[str],
    rollout_context_kind: Optional[str]
):
    segment_should_match = SegmentBuilder(segment_key) \
        .salt(salt) \
        .rules(
            SegmentRuleBuilder() \
                .clauses(make_clause_matching_context(match_context)) \
                .weight(expected_bucket_value + 1) \
                .bucket_by(bucket_by) \
                .rollout_context_kind(rollout_context_kind) \
                .build()
        ) \
        .build()
    segment_should_not_match = SegmentBuilder(segment_key) \
        .salt(salt) \
        .rules(
            SegmentRuleBuilder() \
                .clauses(make_clause_matching_context(match_context)) \
                .weight(expected_bucket_value) \
                .bucket_by(bucket_by) \
                .rollout_context_kind(rollout_context_kind) \
                .build()
        ) \
        .build()
    assert _segment_matches_context(segment_should_match, eval_context) is True
    assert _segment_matches_context(segment_should_not_match, eval_context) is False


def test_explicit_include_user():
    s = {
        "key": "test",
        "included": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_context(s, u) is True

def test_explicit_exclude_user():
    s = {
        "key": "test",
        "excluded": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_context(s, u) is False

def test_explicit_include_has_precedence():
    s = {
        "key": "test",
        "included": [ "foo" ],
        "excluded": [ "foo" ],
        "version": 1
    }
    u = Context.create('foo')
    assert _segment_matches_context(s, u) is True

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
    assert _segment_matches_context(s, u) is True

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
    assert _segment_matches_context(s, u) is True

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
    assert _segment_matches_context(s, u) is True

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
    assert _segment_matches_context(s, u) is False

def test_rollout_calculation_can_bucket_by_key():
    context = Context.builder('userkey').name('Bob').build()
    verify_rollout(context, context, 12551, 'test', 'salt', None, None)

def test_rollout_uses_context_kind():
    context1 = Context.create('key1', 'kind1')
    context2 = Context.create('key2', 'kind2')
    multi = Context.create_multi(context1, context2)
    expected_bucket_value = int(100000 * _bucket_context(None, context2, 'kind2', 'test', 'salt', None))
    verify_rollout(multi, context2, expected_bucket_value, 'test', 'salt', None, 'kind2')

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
    assert _segment_matches_context(s, u) is True

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
    assert _segment_matches_context(s, u) is False
