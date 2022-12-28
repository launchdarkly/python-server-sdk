from ldclient.context import Context
from ldclient.impl.events.event_context_formatter import EventContextFormatter

def test_simple_context():
    f = EventContextFormatter(False, [])
    c = Context.create('a')
    assert f.format_context(c) == {'kind': 'user', 'key': 'a'}

def test_context_with_more_attributes():
    f = EventContextFormatter(False, [])
    c = Context.builder('a').name('b').anonymous(True).set('c', True).set('d', 2).build()
    assert f.format_context(c) == {
        'kind': 'user',
        'key': 'a',
        'name': 'b',
        'anonymous': True,
        'c': True,
        'd': 2
    }

def test_multi_context():
    f = EventContextFormatter(False, [])
    c = Context.create_multi(
        Context.create('a'),
        Context.builder('b').kind('c').name('d').build()
    )
    assert f.format_context(c) == {
        'kind': 'multi',
        'user': {
            'key': 'a'
        },
        'c': {
            'key': 'b',
            'name': 'd'
        }
    }

def test_all_private():
    f = EventContextFormatter(True, [])
    c = Context.builder('a').name('b').anonymous(True).set('c', True).set('d', 2).build()
    assert f.format_context(c) == {
        'kind': 'user',
        'key': 'a',
        'anonymous': True,
        '_meta': {'redactedAttributes': ['name', 'c', 'd']}
    }

def test_some_private_global():
    f = EventContextFormatter(False, ['name', 'd'])
    c = Context.builder('a').name('b').anonymous(True).set('c', True).set('d', 2).build()
    assert f.format_context(c) == {
        'kind': 'user',
        'key': 'a',
        'anonymous': True,
        'c': True,
        '_meta': {'redactedAttributes': ['name', 'd']}
    }

def test_some_private_per_context():
    f = EventContextFormatter(False, ['name'])
    c = Context.builder('a').name('b').anonymous(True).set('c', True).set('d', 2).private('d').build()
    assert f.format_context(c) == {
        'kind': 'user',
        'key': 'a',
        'anonymous': True,
        'c': True,
        '_meta': {'redactedAttributes': ['name', 'd']}
    }

def test_private_property_in_object():
    f = EventContextFormatter(False, ['/b/prop1', '/c/prop2/sub1'])
    c = Context.builder('a') \
        .set('b', {'prop1': True, 'prop2': 3}) \
        .set('c', {'prop1': {'sub1': True}, 'prop2': {'sub1': 4, 'sub2': 5}}) \
        .build()
    assert f.format_context(c) == {
        'kind': 'user',
        'key': 'a',
        'b': {'prop2': 3},
        'c': {'prop1': {'sub1': True}, 'prop2': {'sub2': 5}},
        '_meta': {'redactedAttributes': ['/b/prop1', '/c/prop2/sub1']}
    }
