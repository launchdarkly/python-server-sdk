from ldclient.integrations import Consul

from testing.integrations.persistent_feature_store_test_base import *

have_consul = False
try:
    import consul
    have_consul = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_consul, reason="skipping Consul tests because consul module is not installed")


class ConsulFeatureStoreTester(PersistentFeatureStoreTester):
    def create_persistent_feature_store(self, prefix, caching) -> FeatureStore:
        return Consul.new_feature_store(prefix=prefix, caching=caching)

    def clear_data(self, prefix):
        client = consul.Consul()
        index, keys = client.kv.get((prefix or Consul.DEFAULT_PREFIX) + "/", recurse=True, keys=True)
        for key in (keys or []):
            client.kv.delete(key)

class TestConsulFeatureStore(PersistentFeatureStoreTestBase):
    @property
    def tester_class(self):
        return ConsulFeatureStoreTester


# Consul does not support Big Segments.
