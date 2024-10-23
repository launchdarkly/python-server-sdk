from abc import abstractmethod, abstractproperty

import pytest

from ldclient.feature_store import CacheConfig
from ldclient.interfaces import FeatureStore
from ldclient.testing.feature_store_test_base import (FeatureStoreTestBase,
                                                      FeatureStoreTester,
                                                      StoreTestScope)
from ldclient.testing.test_util import skip_database_tests
from ldclient.versioned_data_kind import FEATURES

# The standard test suite to be run against all persistent feature store implementations. See
# ldclient.testing.feature_store_test_base for the basic model being used here. For each database integration,
# we must define a subclass of PersistentFeatureStoreTester which overrides its abstract methods as
# appropriate for that database, and then define a subclass of PersistentFeatureStoreTestBase which
# simply specifies what tester subclass to use.


class PersistentFeatureStoreTester(FeatureStoreTester):
    def __init__(self):
        self.prefix = None  # type: str
        self.caching = CacheConfig.disabled()

    @abstractmethod
    def create_persistent_feature_store(self, prefix: str, caching: CacheConfig) -> FeatureStore:
        """
        Override this method to create a feature store instance.
        :param prefix: the prefix parameter for the store constructor - may be None or empty to use the default
        :param caching: caching parameters for the store constructor
        """
        pass

    @abstractmethod
    def clear_data(self, prefix: str):
        """
        Override this method to clear any existing data from the database for the specified prefix.
        """
        pass

    def create_feature_store(self) -> FeatureStore:
        return self.create_persistent_feature_store(self.prefix, self.caching)


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class PersistentFeatureStoreTestBase(FeatureStoreTestBase):
    @abstractproperty
    def tester_class(self):
        pass

    @pytest.fixture(params=[(False, False), (True, False), (False, True), (True, True)])
    def tester(self, request):
        specify_prefix, use_caching = request.param
        instance = self.tester_class()
        instance.prefix = "testprefix" if specify_prefix else None
        instance.caching = CacheConfig.default() if use_caching else CacheConfig.disabled()
        return instance

    @pytest.fixture(autouse=True)
    def clear_data_before_each(self, tester):
        tester.clear_data(tester.prefix)

    def test_stores_with_different_prefixes_are_independent(self):
        # This verifies that init(), get(), all(), and upsert() are all correctly using the specified key prefix.
        # The delete() method isn't tested separately because it's implemented as a variant of upsert().
        tester_a = self.tester_class()
        tester_a.prefix = "a"
        tester_a.clear_data(tester_a.prefix)

        tester_b = self.tester_class()
        tester_b.prefix = "b"
        tester_b.clear_data(tester_b.prefix)

        flag_a1 = {'key': 'flagA1', 'version': 1}
        flag_a2 = {'key': 'flagA2', 'version': 1}
        flag_b1 = {'key': 'flagB1', 'version': 1}
        flag_b2 = {'key': 'flagB2', 'version': 1}

        with StoreTestScope(tester_a.create_feature_store()) as store_a:
            with StoreTestScope(tester_b.create_feature_store()) as store_b:
                store_a.init({FEATURES: {'flagA1': flag_a1}})
                store_a.upsert(FEATURES, flag_a2)

                store_b.init({FEATURES: {'flagB1': flag_b1}})
                store_b.upsert(FEATURES, flag_b2)

                item = store_a.get(FEATURES, 'flagA1', lambda x: x)
                assert item == FEATURES.decode(flag_a1)
                item = store_a.get(FEATURES, 'flagB1', lambda x: x)
                assert item is None
                items = store_a.all(FEATURES, lambda x: x)
                assert items == {'flagA1': FEATURES.decode(flag_a1), 'flagA2': FEATURES.decode(flag_a2)}

                item = store_b.get(FEATURES, 'flagB1', lambda x: x)
                assert item == FEATURES.decode(flag_b1)
                item = store_b.get(FEATURES, 'flagA1', lambda x: x)
                assert item is None
                items = store_b.all(FEATURES, lambda x: x)
                assert items == {'flagB1': FEATURES.decode(flag_b1), 'flagB2': FEATURES.decode(flag_b2)}
