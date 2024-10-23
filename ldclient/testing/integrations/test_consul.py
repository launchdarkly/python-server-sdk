import pytest

from ldclient.integrations import Consul
from ldclient.testing.integrations.persistent_feature_store_test_base import *
from ldclient.testing.test_util import skip_database_tests

have_consul = False
try:
    import consul

    have_consul = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_consul, reason="skipping Consul tests because consul module is not installed")


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def consul_defaults_to_available():
    consul = Consul.new_feature_store()
    assert consul.is_monitoring_enabled() is True
    assert consul.is_available() is True


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
def consul_detects_nonexistent_store():
    consul = Consul.new_feature_store(host='http://i-mean-what-are-the-odds')
    assert consul.is_monitoring_enabled() is True
    assert consul.is_available() is False


class ConsulFeatureStoreTester(PersistentFeatureStoreTester):
    def create_persistent_feature_store(self, prefix, caching) -> FeatureStore:
        return Consul.new_feature_store(prefix=prefix, caching=caching)

    def clear_data(self, prefix):
        client = consul.Consul()
        index, keys = client.kv.get((prefix or Consul.DEFAULT_PREFIX) + "/", recurse=True, keys=True)
        for key in keys or []:
            client.kv.delete(key)


class TestConsulFeatureStore(PersistentFeatureStoreTestBase):
    @property
    def tester_class(self):
        return ConsulFeatureStoreTester


# Consul does not support Big Segments.
