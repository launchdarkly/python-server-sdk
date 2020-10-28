import json
from ldclient.client import Config
from ldclient.user_filter import UserFilter


base_config = Config("fake_sdk_key")
config_with_all_attrs_private = Config("fake_sdk_key", all_attributes_private = True)
config_with_some_attrs_private = Config("fake_sdk_key", private_attribute_names=set([u'firstName', u'bizzle']))

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


def test_all_user_attrs_serialized():
    uf = UserFilter(base_config)
    j = uf.filter_user_props(user)
    assert j == user

def test_all_user_attrs_private():
    uf = UserFilter(config_with_all_attrs_private)
    j = uf.filter_user_props(user)
    assert j == user_with_all_attrs_hidden

def test_some_user_attrs_private():
    uf = UserFilter(config_with_some_attrs_private)
    j = uf.filter_user_props(user)
    assert j == user_with_some_attrs_hidden

def test_per_user_private_attr():
    uf = UserFilter(base_config)
    j = uf.filter_user_props(user_specifying_own_private_attr)
    assert j == user_with_own_specified_attr_hidden

def test_per_user_private_attr_plus_global_private_attrs():
    uf = UserFilter(config_with_some_attrs_private)
    j = uf.filter_user_props(user_specifying_own_private_attr)
    assert j == user_with_all_attrs_hidden

def test_unknown_top_level_attrs_stripped():
    uf = UserFilter(base_config)
    j = uf.filter_user_props(user_with_unknown_top_level_attrs)
    assert j == user

def test_leave_anonymous_attr_as_is():
    uf = UserFilter(config_with_all_attrs_private)
    j = uf.filter_user_props(anon_user)
    assert j == anon_user_with_all_attrs_hidden
