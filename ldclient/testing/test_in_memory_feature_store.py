import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureStore
from ldclient.testing.feature_store_test_base import (
    FeatureStoreTestBase,
    FeatureStoreTester
)


def test_in_memory_status_checks():
    store = InMemoryFeatureStore()

    assert not store.is_monitoring_enabled()
    assert store.is_available()


class InMemoryFeatureStoreTester(FeatureStoreTester):
    def create_feature_store(self) -> FeatureStore:
        return InMemoryFeatureStore()


class TestInMemoryFeatureStore(FeatureStoreTestBase):
    @pytest.fixture
    def tester(self):
        return InMemoryFeatureStoreTester()
