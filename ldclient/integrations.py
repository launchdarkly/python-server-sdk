from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.dynamodb_feature_store import _DynamoDBFeatureStoreCore
from ldclient.redis_feature_store import _RedisFeatureStoreCore


class DynamoDB(object):
    """Provides factory methods for integrations between the LaunchDarkly SDK and DynamoDB.
    """
    
    @staticmethod
    def new_feature_store(table_name,
                          prefix=None,
                          dynamodb_opts={},
                          caching=CacheConfig.default()):
        """Creates a DynamoDB-backed implementation of `:class:ldclient.feature_store.FeatureStore`.

        :param string table_name: The name of an existing DynamoDB table
        :param string prefix: An optional namespace prefix to be prepended to all Redis keys
        :param dict dynamodb_opts: Optional parameters for configuring the DynamoDB client, as defined in
          the boto3 API
        :param CacheConfig caching: Specifies whether local caching should be enabled and if so,
          sets the cache properties; defaults to `CacheConfig.default()`
        """
        core = _DynamoDBFeatureStoreCore(table_name, prefix, dynamodb_opts)
        return CachingStoreWrapper(core, caching)


class Redis(object):
    """Provides factory methods for integrations between the LaunchDarkly SDK and Redis.
    """
    DEFAULT_URL = 'redis://localhost:6379/0'
    DEFAULT_PREFIX = 'launchdarkly'
    DEFAULT_MAX_CONNECTIONS = 16
    
    @staticmethod
    def new_feature_store(url='redis://localhost:6379/0',
                          prefix='launchdarkly',
                          max_connections=16,
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
