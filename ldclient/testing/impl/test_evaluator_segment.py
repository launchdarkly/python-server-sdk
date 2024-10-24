import pytest

from ldclient import Context
from ldclient.impl.evaluator import _bucket_context
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *


def _segment_matches_context(segment: Segment, context: Context) -> bool:
    e = EvaluatorBuilder().with_segment(segment).build()
    flag = make_boolean_flag_matching_segment(segment)
    result = e.evaluate(flag, context, event_factory)
    return result.detail.value


def verify_rollout(eval_context: Context, match_context: Context, expected_bucket_value: int, segment_key: str, salt: str, bucket_by: Optional[str], rollout_context_kind: Optional[str]):
    segment_should_match = (
        SegmentBuilder(segment_key)
        .salt(salt)
        .rules(SegmentRuleBuilder().clauses(make_clause_matching_context(match_context)).weight(expected_bucket_value + 1).bucket_by(bucket_by).rollout_context_kind(rollout_context_kind).build())
        .build()
    )
    segment_should_not_match = (
        SegmentBuilder(segment_key)
        .salt(salt)
        .rules(SegmentRuleBuilder().clauses(make_clause_matching_context(match_context)).weight(expected_bucket_value).bucket_by(bucket_by).rollout_context_kind(rollout_context_kind).build())
        .build()
    )
    assert _segment_matches_context(segment_should_match, eval_context) is True
    assert _segment_matches_context(segment_should_not_match, eval_context) is False


def test_explicit_include_user():
    user = Context.create('foo')
    segment = SegmentBuilder('test').included(user.key).build()
    assert _segment_matches_context(segment, user) is True


def test_explicit_exclude_user():
    user = Context.create('foo')
    segment = SegmentBuilder('test').excluded(user.key).rules(make_segment_rule_matching_context(user)).build()
    assert _segment_matches_context(segment, user) is False


def test_explicit_include_has_precedence():
    user = Context.create('foo')
    segment = SegmentBuilder('test').included(user.key).excluded(user.key).build()
    assert _segment_matches_context(segment, user) is True


def test_included_key_for_context_kind():
    c1 = Context.create('key1', 'kind1')
    c2 = Context.create('key2', 'kind2')
    multi = Context.create_multi(c1, c2)
    segment = SegmentBuilder('test').included_contexts('kind1', 'key1').build()
    assert _segment_matches_context(segment, c1) is True
    assert _segment_matches_context(segment, c2) is False
    assert _segment_matches_context(segment, multi) is True


def test_excluded_key_for_context_kind():
    c1 = Context.create('key1', 'kind1')
    c2 = Context.create('key2', 'kind2')
    multi = Context.create_multi(c1, c2)
    segment = SegmentBuilder('test').excluded_contexts('kind1', 'key1').rules(make_segment_rule_matching_context(c1), make_segment_rule_matching_context(c2)).build()
    assert _segment_matches_context(segment, c1) is False
    assert _segment_matches_context(segment, c2) is True
    assert _segment_matches_context(segment, multi) is False


def test_matching_rule_with_no_weight():
    context = Context.create('foo')
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().clauses(make_clause_matching_context(context)).build()).build()
    assert _segment_matches_context(segment, context) is True


def test_matching_rule_with_none_weight():
    context = Context.create('foo')
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().weight(None).clauses(make_clause_matching_context(context)).build()).build()
    assert _segment_matches_context(segment, context) is True


def test_matching_rule_with_full_rollout():
    context = Context.create('foo')
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().weight(100000).clauses(make_clause_matching_context(context)).build()).build()
    assert _segment_matches_context(segment, context) is True


def test_matching_rule_with_zero_rollout():
    context = Context.create('foo')
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().weight(0).clauses(make_clause_matching_context(context)).build()).build()
    assert _segment_matches_context(segment, context) is False


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
    context = Context.builder('foo').name('bob').set('email', 'test@example.com').build()
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().clauses(make_clause(None, 'email', 'in', 'test@example.com'), make_clause(None, 'name', 'in', 'bob')).build()).build()
    assert _segment_matches_context(segment, context) is True


def test_non_matching_rule_with_multiple_clauses():
    context = Context.builder('foo').name('bob').set('email', 'test@example.com').build()
    segment = SegmentBuilder('test').rules(SegmentRuleBuilder().clauses(make_clause(None, 'email', 'in', 'test@example.com'), make_clause(None, 'name', 'in', 'bill')).build()).build()
    assert _segment_matches_context(segment, context) is False


@pytest.mark.parametrize("depth", [1, 2, 3, 4])
def test_segment_cycle_detection(depth: int):
    segment_keys = list("segmentkey%d" % i for i in range(depth))
    segments = []
    for i in range(depth):
        segments.append(SegmentBuilder(segment_keys[i]).rules(SegmentRuleBuilder().clauses(make_clause_matching_segment_key(segment_keys[(i + 1) % depth])).build()).build())
    evaluator_builder = EvaluatorBuilder()
    for s in segments:
        evaluator_builder.with_segment(s)
    evaluator = evaluator_builder.build()
    flag = make_boolean_flag_matching_segment(segments[0])
    context = Context.create('x')
    result = evaluator.evaluate(flag, context, event_factory)
    assert result.detail.value is None
    assert result.detail.reason == {'kind': 'ERROR', 'errorKind': 'MALFORMED_FLAG'}
