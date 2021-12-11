import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureStore

from testing.feature_store_test_base import FeatureStoreTestBase, FeatureStoreTester


class InMemoryFeatureStoreTester(FeatureStoreTester):
    def create_feature_store(self) -> FeatureStore:
        return InMemoryFeatureStore()


class TestInMemoryFeatureStore(FeatureStoreTestBase):
    @pytest.fixture
    def tester(self):
        return InMemoryFeatureStoreTester()
