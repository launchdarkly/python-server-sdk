import math

import pytest

from ldclient.client import Context
from ldclient.impl.evaluator import (_bucket_context,
                                     _variation_index_for_context)
from ldclient.impl.model import *
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *


def assert_match_clause(clause: dict, context: Context, should_match: bool):
    assert_match(basic_evaluator, make_boolean_flag_with_clauses(clause), context, should_match)


class TestEvaluatorBucketing:
    def test_variation_index_is_returned_for_bucket(self):
        user = Context.create('userkey')
        flag = FlagBuilder('key').salt('salt').build()

        # First verify that with our test inputs, the bucket value will be greater than zero and less than 100000,
        # so we can construct a rollout whose second bucket just barely contains that value
        bucket_value = math.trunc(_bucket_context(None, user, None, flag.key, flag.salt, None) * 100000)
        assert bucket_value > 0 and bucket_value < 100000

        bad_variation_a = 0
        matched_variation = 1
        bad_variation_b = 2
        rule = VariationOrRollout(
            {
                'rollout': {
                    'variations': [
                        {'variation': bad_variation_a, 'weight': bucket_value},  # end of bucket range is not inclusive, so it will *not* match the target value
                        {'variation': matched_variation, 'weight': 1},  # size of this bucket is 1, so it only matches that specific value
                        {'variation': bad_variation_b, 'weight': 100000 - (bucket_value + 1)},
                    ]
                }
            }
        )
        result_variation = _variation_index_for_context(flag, rule, user)
        assert result_variation == (matched_variation, False)

    def test_last_bucket_is_used_if_bucket_value_equals_total_weight(self):
        user = Context.create('userkey')
        flag = FlagBuilder('key').salt('salt').build()

        # We'll construct a list of variations that stops right at the target bucket value
        bucket_value = math.trunc(_bucket_context(None, user, None, flag.key, flag.salt, None) * 100000)

        rule = VariationOrRollout({'rollout': {'variations': [{'variation': 0, 'weight': bucket_value}]}})
        result_variation = _variation_index_for_context(flag, rule, user)
        assert result_variation == (0, False)

    def test_bucket_by_user_key(self):
        user = Context.create('userKeyA')
        bucket = _bucket_context(None, user, None, 'hashKey', 'saltyA', None)
        assert bucket == pytest.approx(0.42157587)

        user = Context.create('userKeyB')
        bucket = _bucket_context(None, user, None, 'hashKey', 'saltyA', None)
        assert bucket == pytest.approx(0.6708485)

        user = Context.create('userKeyC')
        bucket = _bucket_context(None, user, None, 'hashKey', 'saltyA', None)
        assert bucket == pytest.approx(0.10343106)

    def test_bucket_by_user_key_with_seed(self):
        seed = 61
        user = Context.create('userKeyA')
        point = _bucket_context(seed, user, None, 'hashKey', 'saltyA', None)
        assert point == pytest.approx(0.09801207)

        user = Context.create('userKeyB')
        point = _bucket_context(seed, user, None, 'hashKey', 'saltyA', None)
        assert point == pytest.approx(0.14483777)

        user = Context.create('userKeyC')
        point = _bucket_context(seed, user, None, 'hashKey', 'saltyA', None)
        assert point == pytest.approx(0.9242641)

    def test_bucket_by_int_attr(self):
        user = Context.builder('userKey').set('intAttr', 33333).set('stringAttr', '33333').build()
        bucket = _bucket_context(None, user, None, 'hashKey', 'saltyA', AttributeRef.from_literal('intAttr'))
        assert bucket == pytest.approx(0.54771423)
        bucket2 = _bucket_context(None, user, None, 'hashKey', 'saltyA', AttributeRef.from_literal('stringAttr'))
        assert bucket2 == bucket

    def test_bucket_by_float_attr_not_allowed(self):
        user = Context.builder('userKey').set('floatAttr', 33.5).build()
        bucket = _bucket_context(None, user, None, 'hashKey', 'saltyA', AttributeRef.from_literal('floatAttr'))
        assert bucket == 0.0

    def test_seed_independent_of_salt_and_hashKey(self):
        seed = 61
        user = Context.create('userKeyA')
        point1 = _bucket_context(seed, user, None, 'hashKey', 'saltyA', None)
        point2 = _bucket_context(seed, user, None, 'hashKey', 'saltyB', None)
        point3 = _bucket_context(seed, user, None, 'hashKey2', 'saltyA', None)

        assert point1 == point2
        assert point2 == point3

    def test_seed_changes_hash_evaluation(self):
        seed1 = 61
        user = Context.create('userKeyA')
        point1 = _bucket_context(seed1, user, None, 'hashKey', 'saltyA', None)
        seed2 = 62
        point2 = _bucket_context(seed2, user, None, 'hashKey', 'saltyB', None)

        assert point1 != point2

    def test_context_kind_selects_context(self):
        seed = 357
        context1 = Context.create('key1')
        context2 = Context.create('key2', 'kind2')
        multi = Context.create_multi(context1, context2)
        key = 'flag-key'
        salt = 'testing123'

        assert _bucket_context(seed, context1, None, key, salt, None) == _bucket_context(seed, context1, 'user', key, salt, None)
        assert _bucket_context(seed, context1, None, key, salt, None) == _bucket_context(seed, multi, 'user', key, salt, None)
        assert _bucket_context(seed, context2, 'kind2', key, salt, None) == _bucket_context(seed, multi, 'kind2', key, salt, None)
        assert _bucket_context(seed, multi, 'user', key, salt, None) != _bucket_context(seed, multi, 'kind2', key, salt, None)
