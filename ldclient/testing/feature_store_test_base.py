from abc import abstractmethod

import pytest

from ldclient.interfaces import FeatureStore
from ldclient.testing.builders import *
from ldclient.versioned_data_kind import FEATURES

# The basic test suite to be run against all feature store implementations.
#
# FeatureStoreTestBase and FeatureStoreTester are used only by test_in_memory_feature_store. For all
# database integrations, see testing.integrations.persistent_feature_store_test_base which extends
# them with additional tests.


class FeatureStoreTester:
    @abstractmethod
    def create_feature_store(self) -> FeatureStore:
        pass


class StoreTestScope:
    def __init__(self, store: FeatureStore):
        self.__store = store

    @property
    def store(self) -> FeatureStore:
        return self.__store

    # These magic methods allow the scope to be automatically cleaned up in a "with" block
    def __enter__(self):
        return self.__store

    def __exit__(self, type, value, traceback):
        if hasattr(self.store, "stop"):  # stop was not originally required for all feature store implementations
            self.__store.stop()


# FeatureStoreTestBase is meant to be used as follows:
# - A subclass adds a pytest fixture called "tester" that will return a series of instances of
#   some subclass of FeatureStoreTester. This allows the entire test suite to be repeated with
#   different store configurations.
# - Tests in this class use "with self.store(tester)" or "with self.inited_store(tester)" to
#   create an instance of the store and ensure that it is torn down afterward.


class FeatureStoreTestBase:
    @abstractmethod
    def all_testers(self):
        pass

    def store(self, tester):
        return StoreTestScope(tester.create_feature_store())

    def inited_store(self, tester):
        scope = StoreTestScope(tester.create_feature_store())
        scope.store.init(
            {
                FEATURES: {
                    'foo': self.make_feature('foo', 10).to_json_dict(),
                    'bar': self.make_feature('bar', 10).to_json_dict(),
                }
            }
        )
        return scope

    @staticmethod
    def make_feature(key, ver):
        return FlagBuilder(key).version(ver).on(True).variations(True, False).salt('abc').build()

    def test_not_initialized_before_init(self, tester):
        with self.store(tester) as store:
            assert store.initialized is False

    def test_initialized(self, tester):
        with self.inited_store(tester) as store:
            assert store.initialized is True

    def test_get_existing_feature(self, tester):
        with self.inited_store(tester) as store:
            expected = self.make_feature('foo', 10)
            flag = store.get(FEATURES, 'foo', lambda x: x)
            assert flag == expected

    def test_get_nonexisting_feature(self, tester):
        with self.inited_store(tester) as store:
            assert store.get(FEATURES, 'biz', lambda x: x) is None

    def test_get_all_versions(self, tester):
        with self.inited_store(tester) as store:
            result = store.all(FEATURES, lambda x: x)
            assert len(result) == 2
            assert result.get('foo') == self.make_feature('foo', 10)
            assert result.get('bar') == self.make_feature('bar', 10)

    def test_upsert_with_newer_version(self, tester):
        with self.inited_store(tester) as store:
            new_ver = self.make_feature('foo', 11)
            store.upsert(FEATURES, new_ver)
            assert store.get(FEATURES, 'foo', lambda x: x) == new_ver

    def test_upsert_with_older_version(self, tester):
        with self.inited_store(tester) as store:
            new_ver = self.make_feature('foo', 9)
            expected = self.make_feature('foo', 10)
            store.upsert(FEATURES, new_ver)
            assert store.get(FEATURES, 'foo', lambda x: x) == expected

    def test_upsert_with_new_feature(self, tester):
        with self.inited_store(tester) as store:
            new_ver = self.make_feature('biz', 1)
            store.upsert(FEATURES, new_ver)
            assert store.get(FEATURES, 'biz', lambda x: x) == new_ver

    def test_delete_with_newer_version(self, tester):
        with self.inited_store(tester) as store:
            store.delete(FEATURES, 'foo', 11)
            assert store.get(FEATURES, 'foo', lambda x: x) is None

    def test_delete_unknown_feature(self, tester):
        with self.inited_store(tester) as store:
            store.delete(FEATURES, 'biz', 11)
            assert store.get(FEATURES, 'biz', lambda x: x) is None

    def test_delete_with_older_version(self, tester):
        with self.inited_store(tester) as store:
            store.delete(FEATURES, 'foo', 9)
            expected = self.make_feature('foo', 10)
            assert store.get(FEATURES, 'foo', lambda x: x) == expected

    def test_upsert_older_version_after_delete(self, tester):
        with self.inited_store(tester) as store:
            store.delete(FEATURES, 'foo', 11)
            old_ver = self.make_feature('foo', 9)
            store.upsert(FEATURES, old_ver)
            assert store.get(FEATURES, 'foo', lambda x: x) is None
