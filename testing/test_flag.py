import pytest
from ldclient.flag import _bucket_user


feature = {
    u'key': u'hashKey',
    u'salt': u'saltyA'
}


def test_bucket_by_user_key():
    user = { u'key': u'userKeyA' }
    bucket = _bucket_user(user, feature, 'key')
    assert bucket == pytest.approx(0.42157587)

    user = { u'key': u'userKeyB' }
    bucket = _bucket_user(user, feature, 'key')
    assert bucket == pytest.approx(0.6708485)

    user = { u'key': u'userKeyC' }
    bucket = _bucket_user(user, feature, 'key')
    assert bucket == pytest.approx(0.10343106)

def test_bucket_by_int_attr():
    user = {
        u'key': u'userKey',
        u'custom': {
            u'intAttr': 33333,
            u'stringAttr': u'33333'
        }
    }
    bucket = _bucket_user(user, feature, 'intAttr')
    assert bucket == pytest.approx(0.54771423)
    bucket2 = _bucket_user(user, feature, 'stringAttr')
    assert bucket2 == bucket

def test_bucket_by_float_attr_not_allowed():
    user = {
        u'key': u'userKey',
        u'custom': {
            u'floatAttr': 33.5
        }
    }
    bucket = _bucket_user(user, feature, 'floatAttr')
    assert bucket == 0.0
