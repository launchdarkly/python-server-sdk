from ldclient.client import InMemoryFeatureStore
import pytest

def make_feature(key, ver):
  return {
      u'key': key,
      u'version': ver,
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

def base_initialized_store():
  store = InMemoryFeatureStore()
  store.init({
    'foo': make_feature('foo', 10),
    'bar': make_feature('bar', 10),
    })
  return store

def test_not_initially_initialized():
  store = InMemoryFeatureStore()
  assert store.initialized == False

def test_initialized():
  store = base_initialized_store()
  assert store.initialized == True

def test_get_existing_feature():
  store = base_initialized_store()
  expected = make_feature('foo', 10)
  assert store.get('foo') == expected

def test_get_nonexisting_feature():
  store = base_initialized_store()
  assert store.get('biz') is None

def test_upsert_with_newer_version():
  store = base_initialized_store()
  new_ver = make_feature('foo', 11)
  store.upsert('foo', new_ver)
  assert store.get('foo') == new_ver

def test_upsert_with_older_version():
  store = base_initialized_store()
  new_ver = make_feature('foo', 9)
  expected = make_feature('foo', 10)
  store.upsert('foo', new_ver)
  assert store.get('foo') == expected

def test_upsert_with_new_feature():
  store = base_initialized_store()
  new_ver = make_feature('biz', 1)
  store.upsert('biz', new_ver)
  assert store.get('biz') == new_ver

def test_delete_with_newer_version():
  store = base_initialized_store()
  store.delete('foo', 11)
  assert store.get('foo') is None

def test_delete_unknown_feature():
  store = base_initialized_store()
  store.delete('biz', 11)
  assert store.get('biz') is None

def test_delete_with_older_version():
  store = base_initialized_store()
  store.delete('foo', 9)
  expected = make_feature('foo', 10)
  assert store.get('foo') == expected