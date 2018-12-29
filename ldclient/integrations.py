from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.redis_feature_store import _RedisFeatureStoreCore


class Redis(object):
    """Provides factory methods for integrations between the LaunchDarkly SDK and Redis,
    """
    DEFAULT_URL = 'redis://localhost:6379/0'
    DEFAULT_PREFIX = 'launchdarkly'
    DEFAULT_MAX_CONNECTIONS = 16
    
    @staticmethod
    def new_feature_store(url=Redis.DEFAULT_URL,
                          prefix=Redis.DEFAULT_PREFIX,
                          max_connections=Redis.DEFAULT_MAX_CONNECTIONS,
                          caching=CacheConfig.default()):
        """Creates a Redis-backed implementation of `:class:ldclient.feature_store.FeatureStore`.

        :param string url: The URL of the Redis host; defaults to `DEFAULT_URL`
        :param string prefix: A namespace prefix to be prepended to all Redis keys; defaults to
          `DEFAULT_PREFIX`
        :param int max_connections: The maximum number of Redis connections to keep in the
          connection pool; defaults to `DEFAULT_MAX_CONNECTIONS`
        :param CacheConfig caching: Specifies whether local caching should be enabled and if so,
          sets the cache properties; defaults to `CacheConfig.default()`
        """
        core = _RedisFeatureStoreCore(url, prefix, max_connections)
        wrapper = CachingStoreWrapper(core, caching)
        wrapper.core = core  # exposed for testing
        return wrapper
