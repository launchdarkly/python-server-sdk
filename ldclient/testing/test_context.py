import json

import pytest

from ldclient.context import Context


def assert_context_valid(c):
    assert c.valid is True
    assert c.error is None


def assert_context_invalid(c):
    assert c.valid is False
    assert c.error is not None


class TestContext:
    def test_create_default_kind(self):
        c = Context.create('a')
        assert_context_valid(c)
        assert c.multiple is False
        assert c.key == 'a'
        assert c.kind == 'user'
        assert c.name is None
        assert c.anonymous is False
        assert list(c.custom_attributes) == []

    def test_create_non_default_kind(self):
        c = Context.create('a', 'b')
        assert_context_valid(c)
        assert c.multiple is False
        assert c.key == 'a'
        assert c.kind == 'b'
        assert c.name is None
        assert c.anonymous is False
        assert list(c.custom_attributes) == []

    def test_builder_default_kind(self):
        c = Context.builder('a').build()
        assert_context_valid(c)
        assert c.multiple is False
        assert c.key == 'a'
        assert c.kind == 'user'
        assert c.name is None
        assert c.anonymous is False
        assert list(c.custom_attributes) == []

    def test_builder_non_default_kind(self):
        c = Context.builder('a').kind('b').build()
        assert_context_valid(c)
        assert c.multiple is False
        assert c.key == 'a'
        assert c.kind == 'b'
        assert c.name is None
        assert c.anonymous is False
        assert list(c.custom_attributes) == []

    def test_name(self):
        c = Context.builder('a').name('b').build()
        assert_context_valid(c)
        assert c.key == 'a'
        assert c.name == 'b'
        assert list(c.custom_attributes) == []

    def test_anonymous(self):
        c = Context.builder('a').anonymous(True).build()
        assert_context_valid(c)
        assert c.key == 'a'
        assert c.anonymous
        assert list(c.custom_attributes) == []

    def test_custom_attributes(self):
        c = Context.builder('a').set('b', True).set('c', 'd').build()
        assert_context_valid(c)
        assert c.key == 'a'
        assert c.get('b') is True
        assert c.get('c') == 'd'
        assert c['b'] is True
        assert c['c'] == 'd'
        assert sorted(list(c.custom_attributes)) == ['b', 'c']

    def test_set_built_in_attribute_by_name(self):
        c = Context.builder('').set('key', 'a').set('kind', 'b').set('name', 'c').set('anonymous', True).build()
        assert_context_valid(c)
        assert c.key == 'a'
        assert c.kind == 'b'
        assert c.name == 'c'
        assert c.anonymous

    def test_set_built_in_attribute_by_name_type_checking(self):
        b = Context.builder('a').kind('b').name('c').anonymous(True)

        assert b.try_set('key', None) is False
        assert b.try_set('key', 3) is False
        assert b.build().key == 'a'

        assert b.try_set('kind', None) is False
        assert b.try_set('kind', 3) is False
        assert b.build().kind == 'b'

        assert b.try_set('name', 3) is False
        assert b.build().name == 'c'

        assert b.try_set('anonymous', None) is False
        assert b.try_set('anonymous', 3) is False
        assert b.build().anonymous is True

    def test_get_built_in_attribute_by_name(self):
        c = Context.builder('a').kind('b').name('c').anonymous(True).build()
        assert c.get('key') == 'a'
        assert c.get('kind') == 'b'
        assert c.get('name') == 'c'
        assert c.get('anonymous') is True

    def test_get_unknown_attribute(self):
        c = Context.create('a')
        assert c.get('b') is None

    def test_private_attributes(self):
        assert list(Context.create('a').private_attributes) == []

        c = Context.builder('a').private('b', '/c/d').private('e').build()
        assert list(c.private_attributes) == ['b', '/c/d', 'e']

    def test_fully_qualified_key(self):
        assert Context.create('key1').fully_qualified_key == 'key1'
        assert Context.create('key1', 'kind1').fully_qualified_key == 'kind1:key1'
        assert Context.create('key%with:things', 'kind1').fully_qualified_key == 'kind1:key%25with%3Athings'

    def test_builder_from_context(self):
        c1 = Context.builder('a').kind('kind1').name('b').set('c', True).private('d').build()
        b = Context.builder_from_context(c1)
        assert b.build() == c1
        b.set('c', False)
        c2 = b.build()
        assert c2 != c1
        assert c1.get('c') is True
        assert c2.get('c') is False

    def test_equality(self):
        def _assert_contexts_from_factory_equal(fn):
            c1, c2 = fn(), fn()
            assert c1 == c2

        _assert_contexts_from_factory_equal(lambda: Context.create('a'))
        _assert_contexts_from_factory_equal(lambda: Context.create('a', 'kind1'))
        _assert_contexts_from_factory_equal(lambda: Context.builder('a').name('b').build())
        _assert_contexts_from_factory_equal(lambda: Context.builder('a').anonymous(True).build())
        _assert_contexts_from_factory_equal(lambda: Context.builder('a').set('b', True).set('c', 3).build())
        assert Context.builder('a').set('b', True).set('c', 3).build() == Context.builder('a').set('c', 3).set('b', True).build()  # order doesn't matter

        assert Context.create('a', 'kind1') != Context.create('b', 'kind1')
        assert Context.create('a', 'kind1') != Context.create('a', 'kind2')
        assert Context.builder('a').name('b').build() != Context.builder('a').name('c').build()
        assert Context.builder('a').anonymous(True).build() != Context.builder('a').build()
        assert Context.builder('a').set('b', True).build() != Context.builder('a').set('b', False).build()
        assert Context.builder('a').set('b', True).build() != Context.builder('a').set('b', True).set('c', False).build()

        _assert_contexts_from_factory_equal(lambda: Context.create_multi(Context.create('a', 'kind1'), Context.create('b', 'kind2')))
        assert Context.create_multi(Context.create('a', 'kind1'), Context.create('b', 'kind2')) == Context.create_multi(
            Context.create('b', 'kind2'), Context.create('a', 'kind1')
        )  # order doesn't matter

        assert Context.create_multi(Context.create('a', 'kind1'), Context.create('b', 'kind2')) != Context.create_multi(Context.create('a', 'kind1'), Context.create('c', 'kind2'))
        assert Context.create_multi(Context.create('a', 'kind1'), Context.create('b', 'kind2'), Context.create('c', 'kind3')) != Context.create_multi(
            Context.create('a', 'kind1'), Context.create('b', 'kind2')
        )
        assert Context.create_multi(Context.create('a', 'kind1'), Context.create('b', 'kind2')) != Context.create('a', 'kind1')

        _assert_contexts_from_factory_equal(lambda: Context.create('invalid', 'kind'))
        assert Context.create('invalid', 'kind') != Context.create_multi()  # different errors

    def test_json_encoding(self):
        assert Context.create('a', 'kind1').to_dict() == {'kind': 'kind1', 'key': 'a'}
        assert Context.builder('a').kind('kind1').name('b').build().to_dict() == {'kind': 'kind1', 'key': 'a', 'name': 'b'}
        assert Context.builder('a').kind('kind1').anonymous(True).build().to_dict() == {'kind': 'kind1', 'key': 'a', 'anonymous': True}
        assert Context.builder('a').kind('kind1').set('b', True).set('c', 3).build().to_dict() == {'kind': 'kind1', 'key': 'a', 'b': True, 'c': 3}
        assert Context.builder('a').kind('kind1').private('b').build().to_dict() == {'kind': 'kind1', 'key': 'a', '_meta': {'privateAttributes': ['b']}}

        assert Context.create_multi(Context.create('key1', 'kind1'), Context.create('key2', 'kind2')).to_dict() == {'kind': 'multi', 'kind1': {'key': 'key1'}, 'kind2': {'key': 'key2'}}

        assert json.loads(Context.create('a', 'kind1').to_json_string()) == {'kind': 'kind1', 'key': 'a'}

    def test_json_decoding(self):
        assert Context.from_dict({'kind': 'kind1', 'key': 'key1'}) == Context.create('key1', 'kind1')
        assert Context.from_dict({'kind': 'kind1', 'key': 'key1', 'name': 'a'}) == Context.builder('key1').kind('kind1').name('a').build()
        assert Context.from_dict({'kind': 'kind1', 'key': 'key1', 'anonymous': True}) == Context.builder('key1').kind('kind1').anonymous(True).build()
        assert Context.from_dict({'kind': 'kind1', 'key': 'key1', '_meta': {'privateAttributes': ['b']}}) == Context.builder('key1').kind('kind1').private('b').build()

        assert Context.from_dict({'kind': 'multi', 'kind1': {'key': 'key1'}, 'kind2': {'key': 'key2'}}) == Context.create_multi(Context.create('key1', 'kind1'), Context.create('key2', 'kind2'))

        assert_context_invalid(Context.from_dict({'kind': 'kind1'}))
        assert_context_invalid(Context.from_dict({'kind': 'kind1', 'key': 3}))
        assert_context_invalid(Context.from_dict({'kind': 'multi'}))
        assert_context_invalid(Context.from_dict({'kind': 'multi', 'kind1': 'x'}))


class TestContextMulti:
    def test_create_multi(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind2')
        mc = Context.create_multi(c1, c2)

        assert mc.valid
        assert mc.multiple
        assert mc.kind == 'multi'
        assert mc.key == ''
        assert mc.name is None
        assert mc.anonymous is False
        assert mc.individual_context_count == 2
        assert mc.get_individual_context(0) is c1
        assert mc.get_individual_context(1) is c2
        assert mc.get_individual_context(-1) is None
        assert mc.get_individual_context(2) is None

    def test_create_multi_flattens_nested_multi_context(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind2')
        c3 = Context.create('c', 'kind3')
        c2plus3 = Context.create_multi(c2, c3)
        mc = Context.create_multi(c1, c2plus3)
        assert mc == Context.create_multi(c1, c2, c3)

    def test_multi_builder(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind2')
        mc = Context.multi_builder().add(c1).add(c2).build()
        assert mc == Context.create_multi(c1, c2)

    def test_multi_builder_flattens_nested_multi_context(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind2')
        c3 = Context.create('c', 'kind3')
        c2plus3 = Context.create_multi(c2, c3)
        mc = Context.multi_builder().add(c1).add(c2plus3).build()
        assert mc == Context.create_multi(c1, c2, c3)

    def test_multi_fully_qualified_key(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind2')
        mc = Context.create_multi(c2, c1)  # deliberately in reverse order of kind - they should come out sorted
        assert mc.fully_qualified_key == 'kind1:a:kind2:b'


class TestContextErrors:
    def test_key_empty_string(self):
        assert_context_invalid(Context.create(''))
        assert_context_invalid(Context.builder('').build())

    @pytest.mark.parametrize('kind', ['kind', 'multi', 'b$c', ''])
    def test_kind_invalid_strings(self, kind):
        assert_context_invalid(Context.create('a', kind))
        assert_context_invalid(Context.builder('a').kind(kind).build())

    def test_create_multi_with_no_contexts(self):
        assert_context_invalid(Context.create_multi())

    def test_multi_builder_with_no_contexts(self):
        assert_context_invalid(Context.multi_builder().build())

    def test_create_multi_with_duplicate_kind(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind1')
        assert_context_invalid(Context.create_multi(c1, c2))

    def test_multi_builder_with_duplicate_kind(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('b', 'kind1')
        assert_context_invalid(Context.multi_builder().add(c1).add(c2).build())

    def test_create_multi_with_invalid_context(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('')
        assert_context_invalid(Context.create_multi(c1, c2))

    def test_multi_builder_with_invalid_context(self):
        c1 = Context.create('a', 'kind1')
        c2 = Context.create('')
        assert_context_invalid(Context.multi_builder().add(c1).add(c2).build())


class TestAnonymousRedaction:
    def test_redacting_anonoymous_leads_to_invalid_context(self):
        original = Context.builder('a').anonymous(True).build()
        c = original.without_anonymous_contexts()

        assert_context_invalid(c)

    def test_redacting_non_anonymous_does_not_change_context(self):
        original = Context.builder('a').anonymous(False).build()
        c = original.without_anonymous_contexts()

        assert_context_valid(c)
        assert c == original

    def test_can_find_non_anonymous_contexts_from_multi(self):
        anon = Context.builder('a').anonymous(True).build()
        nonanon = Context.create('b', 'kind2')
        mc = Context.create_multi(anon, nonanon)

        filtered = mc.without_anonymous_contexts()

        assert_context_valid(filtered)
        assert filtered.individual_context_count == 1
        assert filtered.key == 'b'
        assert filtered.kind == 'kind2'

    def test_can_filter_all_from_multi(self):
        a = Context.builder('a').anonymous(True).build()
        b = Context.builder('b').anonymous(True).build()
        mc = Context.create_multi(a, b)

        filtered = mc.without_anonymous_contexts()

        assert_context_invalid(filtered)
