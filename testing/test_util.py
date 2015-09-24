from __future__ import division
import hashlib
from copy import copy
from math import floor
from ldclient import util

minimal_feature = {
    u'key': u'feature.key',
    u'salt': u'abc',
    u'on': True,
    u'variations': [
        {
            u'value': True,
            u'weight': 100,
            u'targets': []
        },
        {
            u'value': False,
            u'weight': 0,
            u'targets': []
        }
    ]
}

user = {
    u'key': u'xyz',
    u'custom': {
        u'bizzle': u'def'
    }
}


def test_param_for_user_with_no_key():
    assert util._param_for_user(minimal_feature, {}) is None


def test_param_for_user_with_no_secondary():
    expected = int(hashlib.sha1('feature.key.abc.xyz'.encode('utf-8')).hexdigest()[:15], 16) / float(0xFFFFFFFFFFFFFFF)
    assert util._param_for_user(minimal_feature, {u'key': u'xyz'}) == expected


def test_match_target_key_mismatch():
    target = {
        u'attribute': u'key',
        u'op': u'in',
        u'values': ['lmno']
    }
    assert util._match_target(target, {'key': 'xyz'}) == False


def test_match_target_key_empty():
    target = {
        u'attribute': u'key',
        u'op': u'in',
        u'values': []
    }
    assert util._match_target(target, {'key': 'xyz'}) == False


def test_match_target_key_match():
    target = {
        u'attribute': u'key',
        u'op': u'in',
        u'values': ['xyz']
    }
    assert util._match_target(target, {'key': 'xyz'}) == True


def test_match_target_custom_match():
    target = {
        u'attribute': u'bizzle',
        u'op': u'in',
        u'values': [u'def']
    }
    assert util._match_target(target, user) == True


def test_match_target_custom_mismatch():
    target = {
        u'attribute': u'bizzle',
        u'op': u'in',
        u'values': [u'ghi']
    }
    assert util._match_target(target, user) == False


def test_match_target_custom_attribute_mismatch():
    target = {
        u'attribute': u'bazzle',
        u'op': u'in',
        u'values': [u'def']
    }
    assert util._match_target(target, user) == False


def test_match_variation_target_match():
    variation = {
        u'userTarget': {
            u'attribute': u'key',
            u'op': u'in',
            u'values': []
        },
        u'targets': [
            {
                u'attribute': u'bazzle',
                u'op': u'in',
                u'values': [u'zyx']
            },
            {
                u'attribute': u'bizzle',
                u'op': u'in',
                u'values': [u'def']
            }
        ]
    }
    assert util._match_variation(variation, user) == True


def test_match_variation_target_mismatch():
    variation = {
        u'userTarget': {
            u'attribute': u'key',
            u'op': u'in',
            u'values': []
        },
        u'targets': [
            {
                u'attribute': u'bazzle',
                u'op': u'in',
                u'values': [u'zyx']
            },
            {
                u'attribute': u'bizzle',
                u'op': u'in',
                u'values': [u'abc']
            }
        ]
    }
    assert util._match_variation(variation, user) == False


def test_evaluate_feature_off():
    feature = copy(minimal_feature)
    feature['on'] = False
    assert util._evaluate(feature, user) == None


def test_evaluate_first_variation_target_match():
    feature = copy(minimal_feature)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': 0,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': []
            },
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': []
            },
            u'targets': []
        }
    ]
    assert util._evaluate(feature, user) == True


def test_evaluate_first_variation_both_targets_match():
    feature = copy(minimal_feature)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': 0,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': []
            },
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': []
            },
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == True


def test_evaluate_first_variation_both_targets_match_user_key_match_no_user_target():
    feature = copy(minimal_feature)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': 0,
            u'targets': [
                {
                    u'attribute': u'key',
                    u'op': u'in',
                    u'values': ['xyz']
                },
            ]
        },
        {
            u'value': False,
            u'weight': 100,
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == True


def test_evaluate_second_variation_user_match_both_targets_match():
    feature = copy(minimal_feature)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': 0,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': []
            },
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100,
            u'userTarget': {
                u'attribute': u'key',
                u'op': u'in',
                u'values': ['xyz']
            },
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == False


def test_evaluate_second_variation_target_match():
    feature = copy(minimal_feature)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': 0,
            u'targets': [
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'defg']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100,
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'def']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == False


def test_evaluate_first_variation_no_target_match():
    feature = copy(minimal_feature)
    hash_value = 100 * int(hashlib.sha1('feature.key.abc.xyz'.encode('utf-8')).hexdigest()[:15], 16) / float(
        0xFFFFFFFFFFFFFFF)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': floor(hash_value) + 1,
            u'targets': [
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'defg']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100 - (floor(hash_value) + 1),
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'defg']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == True


def test_evaluate_second_variation_no_target_match():
    feature = copy(minimal_feature)
    hash_value = int(hashlib.sha1('feature.key.abc.xyz'.encode('utf-8')).hexdigest()[:15], 16) / float(
        0xFFFFFFFFFFFFFFF)
    feature['variations'] = [
        {
            u'value': True,
            u'weight': floor(hash_value) - 1,
            u'targets': [
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'defg']
                }
            ]
        },
        {
            u'value': False,
            u'weight': 100 - (floor(hash_value) - 1),
            u'targets': [
                {
                    u'attribute': u'bazzle',
                    u'op': u'in',
                    u'values': [u'zyx']
                },
                {
                    u'attribute': u'bizzle',
                    u'op': u'in',
                    u'values': [u'defg']
                }
            ]
        }
    ]
    assert util._evaluate(feature, user) == False

