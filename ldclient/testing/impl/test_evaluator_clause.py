from ldclient.client import Context
from ldclient.testing.builders import *
from ldclient.testing.impl.evaluator_util import *


def assert_match_clause(clause: dict, context: Context, should_match: bool):
    assert_match(basic_evaluator, make_boolean_flag_with_clauses(clause), context, should_match)


class TestEvaluatorClause:
    def test_match_built_in_attribute(self):
        clause = make_clause(None, 'name', 'in', 'Bob')
        context = Context.builder('key').name('Bob').build()
        assert_match_clause(clause, context, True)

    def test_match_custom_attribute(self):
        clause = make_clause(None, 'legs', 'in', 4)
        context = Context.builder('key').set('legs', 4).build()
        assert_match_clause(clause, context, True)

    def test_missing_attribute(self):
        clause = make_clause(None, 'legs', 'in', '4')
        context = Context.create('key')
        assert_match_clause(clause, context, False)

    def test_match_context_value_to_any_of_multiple_values(self):
        clause = make_clause(None, 'name', 'in', 'Bob', 'Carol')
        context = Context.builder('key').name('Carol').build()
        assert_match_clause(clause, context, True)

    def test_match_array_of_context_values_to_clause_value(self):
        clause = make_clause(None, 'alias', 'in', 'Maurice')
        context = Context.builder('key').set('alias', ['Space Cowboy', 'Maurice']).build()
        assert_match_clause(clause, context, True)

    def test_no_match_in_array_of_context_values(self):
        clause = make_clause(None, 'alias', 'in', 'Ma')
        context = Context.builder('key').set('alias', ['Mary', 'May']).build()
        assert_match_clause(clause, context, False)

    def test_negated_to_return_false(self):
        clause = negate_clause(make_clause(None, 'name', 'in', 'Bob'))
        context = Context.builder('key').name('Bob').build()
        assert_match_clause(clause, context, False)

    def test_negated_to_return_true(self):
        clause = negate_clause(make_clause(None, 'name', 'in', 'Bobby'))
        context = Context.builder('key').name('Bob').build()
        assert_match_clause(clause, context, True)

    def test_unknown_operator_does_not_match(self):
        clause = make_clause(None, 'name', 'doesSomethingUnsupported', 'Bob')
        context = Context.builder('key').name('Bob').build()
        assert_match_clause(clause, context, False)

    def test_clause_match_uses_context_kind(self):
        clause = make_clause('company', 'name', 'in', 'Catco')
        context1 = Context.builder('cc').kind('company').name('Catco').build()
        context2 = Context.builder('l').name('Lucy').build()
        context3 = Context.create_multi(context1, context2)
        assert_match_clause(clause, context1, True)
        assert_match_clause(clause, context2, False)
        assert_match_clause(clause, context3, True)

    def test_clause_match_by_kind_attribute(self):
        clause = make_clause(None, 'kind', 'startsWith', 'a')
        context1 = Context.create('key')
        context2 = Context.create('key', 'ab')
        context3 = Context.create_multi(Context.create('key', 'cd'), context2)
        assert_match_clause(clause, context1, False)
        assert_match_clause(clause, context2, True)
        assert_match_clause(clause, context3, True)
