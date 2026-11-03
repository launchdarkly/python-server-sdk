import pytest

from ldclient.integrations import Consul
from ldclient.testhelpers.storetest import FeatureStoreCoreTestSuite
from ldclient.testing.test_util import skip_database_tests

have_consul = False
try:
    import consul

    from ldclient.impl.integrations.consul.consul_feature_store import (
        _ConsulFeatureStoreCore
    )

    have_consul = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not have_consul, reason="skipping Consul tests because consul module is not installed")


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class TestConsulFeatureStoreCore(FeatureStoreCoreTestSuite):
    def create_core(self, prefix):
        return _ConsulFeatureStoreCore(None, None, prefix, {})

    def clear_data(self, prefix):
        client = consul.Consul()
        index, keys = client.kv.get((prefix or Consul.DEFAULT_PREFIX) + "/", recurse=True, keys=True)
        for key in keys or []:
            client.kv.delete(key)

    def create_error_core(self):
        # Nothing listens on this port, so each operation fails.
        return _ConsulFeatureStoreCore('localhost', 1, None, {})


# Consul does not support Big Segments.
