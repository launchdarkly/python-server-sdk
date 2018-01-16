from builtins import object
import json
from ldclient.client import Config
from ldclient.event_serializer import EventSerializer


base_config = Config()
config_with_all_attrs_private = Config(all_attributes_private = True)
config_with_some_attrs_private = Config(private_attribute_names=[u'firstName', u'bizzle'])

# users to serialize

user = {
    u'key': u'abc',
    u'firstName': u'Sue',
    u'custom': {
        u'bizzle': u'def',
        u'dizzle': u'ghi'
    }
}

user_specifying_own_private_attr = {
    u'key': u'abc',
    u'firstName': u'Sue',
    u'custom': {
        u'bizzle': u'def',
        u'dizzle': u'ghi'
    },
    u'privateAttributeNames': [ u'dizzle', u'unused' ]
}

user_with_unknown_top_level_attrs = {
    u'key': u'abc',
    u'firstName': u'Sue',
    u'species': u'human',
    u'hatSize': 6,
    u'custom': {
        u'bizzle': u'def',
        u'dizzle': u'ghi'
    }
}

anon_user = {    
    u'key': u'abc',
    u'anonymous': True,
    u'custom': {
        u'bizzle': u'def',
        u'dizzle': u'ghi'
    }
}

# expected results from serializing user

user_with_all_attrs_hidden = {
    u'key': u'abc',
    u'custom': { },
    u'privateAttrs': [ u'bizzle', u'dizzle',  u'firstName' ]
}

user_with_some_attrs_hidden = {
    u'key': u'abc',
    u'custom': {
        u'dizzle': u'ghi'
    },
    u'privateAttrs': [ u'bizzle',  u'firstName' ]
}

user_with_own_specified_attr_hidden = {
    u'key': u'abc',
    u'firstName': u'Sue',
    u'custom': {
        u'bizzle': u'def'
    },
    u'privateAttrs': [ u'dizzle' ]
}

anon_user_with_all_attrs_hidden = {
    u'key': u'abc',
    u'anonymous': True,
    u'custom': { },
    u'privateAttrs': [ u'bizzle', u'dizzle' ]
}

def make_event(u, key = u'xyz'):
    return {
        u'creationDate': 1000000,
        u'key': key,
        u'kind': u'thing',
        u'user': u
    }


def test_all_user_attrs_serialized():
    es = EventSerializer(base_config)
    event = make_event(user)
    j = es.serialize_events(event)
    assert json.loads(j) == [event]

def test_all_user_attrs_private():
    es = EventSerializer(config_with_all_attrs_private)
    event = make_event(user)
    filtered_event = make_event(user_with_all_attrs_hidden)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]

def test_some_user_attrs_private():
    es = EventSerializer(config_with_some_attrs_private)
    event = make_event(user)
    filtered_event = make_event(user_with_some_attrs_hidden)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]

def test_per_user_private_attr():
    es = EventSerializer(base_config)
    event = make_event(user_specifying_own_private_attr)
    filtered_event = make_event(user_with_own_specified_attr_hidden)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]

def test_per_user_private_attr_plus_global_private_attrs():
    es = EventSerializer(config_with_some_attrs_private)
    event = make_event(user_specifying_own_private_attr)
    filtered_event = make_event(user_with_all_attrs_hidden)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]

def test_all_events_serialized():
    es = EventSerializer(config_with_all_attrs_private)
    event0 = make_event(user, 'key0')
    event1 = make_event(user, 'key1')
    filtered0 = make_event(user_with_all_attrs_hidden, 'key0')
    filtered1 = make_event(user_with_all_attrs_hidden, 'key1')
    j = es.serialize_events([event0, event1])
    assert json.loads(j) == [filtered0, filtered1]

def test_unknown_top_level_attrs_stripped():
    es = EventSerializer(base_config)
    event = make_event(user_with_unknown_top_level_attrs)
    filtered_event = make_event(user)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]

def test_leave_anonymous_attr_as_is():
    es = EventSerializer(config_with_all_attrs_private)
    event = make_event(anon_user)
    filtered_event = make_event(anon_user_with_all_attrs_hidden)
    j = es.serialize_events(event)
    assert json.loads(j) == [filtered_event]
