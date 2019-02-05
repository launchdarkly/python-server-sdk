from ldclient.impl.integrations.redis.redis_feature_store import _RedisFeatureStoreCore

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.interfaces import FeatureStore


# Note that this class is now just a facade around CachingStoreWrapper, which is in turn delegating
# to _RedisFeatureStoreCore where the actual database logic is. This class was retained for historical
# reasons, to support existing code that calls the RedisFeatureStore constructor. In the future, we
# will migrate away from exposing these concrete classes and use only the factory methods.

class RedisFeatureStore(FeatureStore):
    """A Redis-backed implementation of :class:`ldclient.interfaces.FeatureStore`.

    .. deprecated:: 6.7.0
      This module and this implementation class are deprecated and may be changed or removed in the future.
      Please use :func:`ldclient.integrations.Redis.new_feature_store()`.
    """
    def __init__(self,
                 url='redis://localhost:6379/0',
                 prefix='launchdarkly',
                 max_connections=16,
                 expiration=15,
                 capacity=1000):
        self.core = _RedisFeatureStoreCore(url, prefix, max_connections)  # exposed for testing
        self._wrapper = CachingStoreWrapper(self.core, CacheConfig(expiration=expiration, capacity=capacity))

    def get(self, kind, key, callback = lambda x: x):
        return self._wrapper.get(kind, key, callback)
    
    def all(self, kind, callback):
        return self._wrapper.all(kind, callback)
    
    def init(self, all_data):
        return self._wrapper.init(all_data)
    
    def upsert(self, kind, item):
        return self._wrapper.upsert(kind, item)
    
    def delete(self, kind, key, version):
        return self._wrapper.delete(kind, key, version)
    
    @property
    def initialized(self):
        return self._wrapper.initialized
