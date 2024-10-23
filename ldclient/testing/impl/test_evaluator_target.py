from ldclient.client import Context
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *

FALLTHROUGH_VAR = 0
MATCH_VAR_1 = 1
MATCH_VAR_2 = 2
VARIATIONS = ['fallthrough', 'match1', 'match2']


def assert_match_clause(clause: dict, context: Context, should_match: bool):
    assert_match(basic_evaluator, make_boolean_flag_with_clauses(clause), context, should_match)


def base_flag_builder() -> FlagBuilder:
    return FlagBuilder('feature').on(True).variations(*VARIATIONS).fallthrough_variation(FALLTHROUGH_VAR).off_variation(FALLTHROUGH_VAR)


def expect_match(flag: FeatureFlag, context: Context, variation: int):
    result = basic_evaluator.evaluate(flag, context, event_factory)
    assert result.detail.variation_index == variation
    assert result.detail.value == VARIATIONS[variation]
    assert result.detail.reason == {'kind': 'TARGET_MATCH'}


def expect_fallthrough(flag: FeatureFlag, context: Context):
    result = basic_evaluator.evaluate(flag, context, event_factory)
    assert result.detail.variation_index == FALLTHROUGH_VAR
    assert result.detail.value == VARIATIONS[FALLTHROUGH_VAR]
    assert result.detail.reason == {'kind': 'FALLTHROUGH'}


class TestEvaluatorTarget:
    def test_user_targets_only(self):
        flag = base_flag_builder().target(MATCH_VAR_1, 'c').target(MATCH_VAR_2, 'b', 'a').build()

        expect_match(flag, Context.create('a'), MATCH_VAR_2)
        expect_match(flag, Context.create('b'), MATCH_VAR_2)
        expect_match(flag, Context.create('c'), MATCH_VAR_1)
        expect_fallthrough(flag, Context.create('z'))

        # in a multi-kind context, these targets match only the key for the user kind
        expect_match(flag, Context.create_multi(Context.create('b', 'dog'), Context.create('a')), MATCH_VAR_2)
        expect_match(flag, Context.create_multi(Context.create('a', 'dog'), Context.create('c')), MATCH_VAR_1)
        expect_fallthrough(flag, Context.create_multi(Context.create('b', 'dog'), Context.create('z')))
        expect_fallthrough(flag, Context.create_multi(Context.create('a', 'dog'), Context.create('b', 'cat')))

    def test_user_targets_and_context_targets(self):
        flag = (
            base_flag_builder()
            .target(MATCH_VAR_1, 'c')
            .target(MATCH_VAR_2, 'b', 'a')
            .context_target('dog', MATCH_VAR_1, 'a', 'b')
            .context_target('dog', MATCH_VAR_2, 'c')
            .context_target(Context.DEFAULT_KIND, MATCH_VAR_1)
            .context_target(Context.DEFAULT_KIND, MATCH_VAR_2)
            .build()
        )

        expect_match(flag, Context.create('a'), MATCH_VAR_2)
        expect_match(flag, Context.create('b'), MATCH_VAR_2)
        expect_match(flag, Context.create('c'), MATCH_VAR_1)
        expect_fallthrough(flag, Context.create('z'))

        expect_match(flag, Context.create_multi(Context.create('b', 'dog'), Context.create('a')), MATCH_VAR_1)  # the "dog" target takes precedence due to ordering
        expect_match(flag, Context.create_multi(Context.create('z', 'dog'), Context.create('a')), MATCH_VAR_2)  # "dog" targets don't match, continue to "user" targets
        expect_fallthrough(flag, Context.create_multi(Context.create('x', 'dog'), Context.create('z')))  # nothing matches
        expect_match(flag, Context.create_multi(Context.create('a', 'dog'), Context.create('b', 'cat')), MATCH_VAR_1)
