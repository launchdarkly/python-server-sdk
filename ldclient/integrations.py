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

        To use this method, you must first install the `boto3` package containing the AWS SDK gems.
        Then, put the object returned by this method into the `feature_store` property of your
        client configuration (:class:ldclient.config.Config).

        Note that the DynamoDB table must already exist; the LaunchDarkly SDK does not create the table
        automatically, because it has no way of knowing what additional properties (such as permissions
        and throughput) you would want it to have. The table must have a partition key called
        "namespace" and a sort key called "key", both with a string type.

        By default, the DynamoDB client will try to get your AWS credentials and region name from
        environment variables and/or local configuration files, as described in the AWS SDK documentation.
        You may also pass configuration settings in `dynamodb_opts`.

        :param string table_name: The name of an existing DynamoDB table
        :param string prefix: An optional namespace prefix to be prepended to all DynamoDB keys
        :param dict dynamodb_opts: Optional parameters for configuring the DynamoDB client, as defined in
          the boto3 API; see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
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

        To use this method, you must first install the `redis` package. Then, put the object
        returned by this method into the `feature_store` property of your client configuration
        (:class:ldclient.config.Config).

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
