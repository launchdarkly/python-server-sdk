"""
This submodule contains factory/configuration methods for integrating the SDK with services
other than LaunchDarkly.
"""

from typing import Any, Dict, List, Mapping, Optional

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.impl.integrations.consul.consul_feature_store import \
    _ConsulFeatureStoreCore
from ldclient.impl.integrations.dynamodb.dynamodb_big_segment_store import \
    _DynamoDBBigSegmentStore
from ldclient.impl.integrations.dynamodb.dynamodb_feature_store import \
    _DynamoDBFeatureStoreCore
from ldclient.impl.integrations.files.file_data_source import _FileDataSource
from ldclient.impl.integrations.redis.redis_big_segment_store import \
    _RedisBigSegmentStore
from ldclient.impl.integrations.redis.redis_feature_store import \
    _RedisFeatureStoreCore
from ldclient.interfaces import BigSegmentStore


class Consul:
    """Provides factory methods for integrations between the LaunchDarkly SDK and Consul."""

    """The key prefix that is used if you do not specify one."""
    DEFAULT_PREFIX = "launchdarkly"

    @staticmethod
    def new_feature_store(
        host: Optional[str] = None, port: Optional[int] = None, prefix: Optional[str] = None, consul_opts: Optional[dict] = None, caching: CacheConfig = CacheConfig.default()
    ) -> CachingStoreWrapper:
        """Creates a Consul-backed implementation of :class:`ldclient.interfaces.FeatureStore`.
        For more details about how and why you can use a persistent feature store, see the
        `SDK reference guide <https://docs.launchdarkly.com/sdk/concepts/data-stores>`_.

        To use this method, you must first install the ``python-consul`` package. Then, put the object
        returned by this method into the ``feature_store`` property of your client configuration
        (:class:`ldclient.config.Config`).
        ::

            from ldclient.integrations import Consul
            store = Consul.new_feature_store()
            config = Config(feature_store=store)

        :param host: hostname of the Consul server (uses ``localhost`` if omitted)
        :param port: port of the Consul server (uses 8500 if omitted)
        :param prefix: a namespace prefix to be prepended to all Consul keys
        :param consul_opts: optional parameters for configuring the Consul client, if you need
          to set any of them besides host and port, as defined in the
          `python-consul API <https://python-consul.readthedocs.io/en/latest/#consul>`_
        :param caching: specifies whether local caching should be enabled and if so,
          sets the cache properties; defaults to :func:`ldclient.feature_store.CacheConfig.default()`
        """
        core = _ConsulFeatureStoreCore(host, port, prefix, consul_opts)
        return CachingStoreWrapper(core, caching)


class DynamoDB:
    """Provides factory methods for integrations between the LaunchDarkly SDK and DynamoDB."""

    @staticmethod
    def new_feature_store(table_name: str, prefix: Optional[str] = None, dynamodb_opts: Mapping[str, Any] = {}, caching: CacheConfig = CacheConfig.default()) -> CachingStoreWrapper:
        """Creates a DynamoDB-backed implementation of :class:`ldclient.interfaces.FeatureStore`.
        For more details about how and why you can use a persistent feature store, see the
        `SDK reference guide <https://docs.launchdarkly.com/sdk/concepts/data-stores>`_.

        To use this method, you must first install the ``boto3`` package for the AWS SDK.
        Then, put the object returned by this method into the ``feature_store`` property of your
        client configuration (:class:`ldclient.config.Config`).
        ::

            from ldclient.integrations import DynamoDB
            store = DynamoDB.new_feature_store("my-table-name")
            config = Config(feature_store=store)

        Note that the DynamoDB table must already exist; the LaunchDarkly SDK does not create the table
        automatically, because it has no way of knowing what additional properties (such as permissions
        and throughput) you would want it to have. The table must have a partition key called
        "namespace" and a sort key called "key", both with a string type.

        By default, the DynamoDB client will try to get your AWS credentials and region name from
        environment variables and/or local configuration files, as described in the AWS SDK documentation.
        You may also pass configuration settings in ``dynamodb_opts``.

        :param table_name: the name of an existing DynamoDB table
        :param prefix: an optional namespace prefix to be prepended to all DynamoDB keys
        :param dynamodb_opts: optional parameters for configuring the DynamoDB client, as defined in
          the `boto3 API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_
        :param caching: specifies whether local caching should be enabled and if so,
          sets the cache properties; defaults to :func:`ldclient.feature_store.CacheConfig.default()`
        """
        core = _DynamoDBFeatureStoreCore(table_name, prefix, dynamodb_opts)
        return CachingStoreWrapper(core, caching)

    @staticmethod
    def new_big_segment_store(table_name: str, prefix: Optional[str] = None, dynamodb_opts: Mapping[str, Any] = {}):
        """
        Creates a DynamoDB-backed Big Segment store.

        Big Segments are a specific type of user segments. For more information, read the LaunchDarkly
        documentation: https://docs.launchdarkly.com/home/users/big-segments

        To use this method, you must first install the ``boto3`` package for the AWS SDK. Then,
        put the object returned by this method into the ``store`` property of your Big Segments
        configuration (see :class:`ldclient.config.Config`).
        ::

          from ldclient.config import Config, BigSegmentsConfig
          from ldclient.integrations import DynamoDB
          store = DynamoDB.new_big_segment_store("my-table-name")
          config = Config(big_segments=BigSegmentsConfig(store=store))

        Note that the DynamoDB table must already exist; the LaunchDarkly SDK does not create the table
        automatically, because it has no way of knowing what additional properties (such as permissions
        and throughput) you would want it to have. The table must have a partition key called
        "namespace" and a sort key called "key", both with a string type.

        By default, the DynamoDB client will try to get your AWS credentials and region name from
        environment variables and/or local configuration files, as described in the AWS SDK documentation.
        You may also pass configuration settings in ``dynamodb_opts``.

        :param table_name: the name of an existing DynamoDB table
        :param prefix: an optional namespace prefix to be prepended to all DynamoDB keys
        :param dynamodb_opts: optional parameters for configuring the DynamoDB client, as defined in
          the `boto3 API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client>`_
        """
        return _DynamoDBBigSegmentStore(table_name, prefix, dynamodb_opts)


class Redis:
    """Provides factory methods for integrations between the LaunchDarkly SDK and Redis."""

    DEFAULT_URL = 'redis://localhost:6379/0'
    DEFAULT_PREFIX = 'launchdarkly'
    DEFAULT_MAX_CONNECTIONS = 16

    @staticmethod
    def new_feature_store(
        url: str = 'redis://localhost:6379/0', prefix: str = 'launchdarkly', max_connections: int = 16, caching: CacheConfig = CacheConfig.default(), redis_opts: Dict[str, Any] = {}
    ) -> CachingStoreWrapper:
        """
        Creates a Redis-backed implementation of :class:`~ldclient.interfaces.FeatureStore`.
        For more details about how and why you can use a persistent feature store, see the
        `SDK reference guide <https://docs.launchdarkly.com/sdk/concepts/data-stores>`_.

        To use this method, you must first install the ``redis`` package. Then, put the object
        returned by this method into the ``feature_store`` property of your client configuration
        (:class:`ldclient.config.Config`).
        ::

            from ldclient.config import Config
            from ldclient.integrations import Redis
            store = Redis.new_feature_store()
            config = Config(feature_store=store)

        :param url: the URL of the Redis host; defaults to ``DEFAULT_URL``
        :param prefix: a namespace prefix to be prepended to all Redis keys; defaults to
          ``DEFAULT_PREFIX``
        :param caching: specifies whether local caching should be enabled and if so,
          sets the cache properties; defaults to :func:`ldclient.feature_store.CacheConfig.default()`
        :param redis_opts: extra options for initializing Redis connection from the url,
          see `redis.connection.ConnectionPool.from_url` for more details.
        """

        core = _RedisFeatureStoreCore(url, prefix, redis_opts)
        wrapper = CachingStoreWrapper(core, caching)
        wrapper._core = core  # exposed for testing
        return wrapper

    @staticmethod
    def new_big_segment_store(url: str = 'redis://localhost:6379/0', prefix: str = 'launchdarkly', max_connections: int = 16, redis_opts: Dict[str, Any] = {}) -> BigSegmentStore:
        """
        Creates a Redis-backed Big Segment store.

        Big Segments are a specific type of user segments. For more information, read the LaunchDarkly
        documentation: https://docs.launchdarkly.com/home/users/big-segments

        To use this method, you must first install the ``redis`` package. Then, put the object
        returned by this method into the ``store`` property of your Big Segments configuration
        (see :class:`ldclient.config.Config`).
        ::

          from ldclient.config import Config, BigSegmentsConfig
          from ldclient.integrations import Redis
          store = Redis.new_big_segment_store()
          config = Config(big_segments=BigSegmentsConfig(store=store))

        :param url: the URL of the Redis host; defaults to ``DEFAULT_URL``
        :param prefix: a namespace prefix to be prepended to all Redis keys; defaults to
          ``DEFAULT_PREFIX``
        :param redis_opts: extra options for initializing Redis connection from the url,
          see `redis.connection.ConnectionPool.from_url` for more details.
        """

        return _RedisBigSegmentStore(url, prefix, redis_opts)


class Files:
    """Provides factory methods for integrations with filesystem data."""

    @staticmethod
    def new_data_source(paths: List[str], auto_update: bool = False, poll_interval: float = 1, force_polling: bool = False) -> object:
        """Provides a way to use local files as a source of feature flag state. This would typically be
        used in a test environment, to operate using a predetermined feature flag state without an
        actual LaunchDarkly connection.

        To use this component, call ``new_data_source``, specifying the file path(s) of your data file(s)
        in the ``paths`` parameter; then put the value returned by this method into the ``update_processor_class``
        property of your LaunchDarkly client configuration (:class:`ldclient.config.Config`).
        ::

            from ldclient.integrations import Files
            data_source = Files.new_data_source(paths=[ myFilePath ])
            config = Config(update_processor_class=data_source)

        This will cause the client not to connect to LaunchDarkly to get feature flags. The
        client may still make network connections to send analytics events, unless you have disabled
        this in your configuration with ``send_events`` or ``offline``.

        The format of the data files is described in the SDK Reference Guide on
        `Reading flags from a file <https://docs.launchdarkly.com/sdk/features/flags-from-files#python>`_.
        Note that in order to use YAML, you will need to install the ``pyyaml`` package.

        If the data source encounters any error in any file-- malformed content, a missing file, or a
        duplicate key-- it will not load flags from any of the files.

        :param paths: the paths of the source files for loading flag data. These may be absolute paths
          or relative to the current working directory. Files will be parsed as JSON unless the ``pyyaml``
          package is installed, in which case YAML is also allowed.
        :param auto_update: (default: false) True if the data source should watch for changes to the source file(s)
          and reload flags whenever there is a change. The default implementation of this feature is based on
          polling the filesystem, which may not perform well; if you install the ``watchdog`` package, its
          native file watching mechanism will be used instead. Note that auto-updating will only work if all
          of the files you specified have valid directory paths at startup time.
        :param poll_interval: (default: 1) the minimum interval, in seconds, between checks for file
          modifications-- used only if ``auto_update`` is true, and if the native file-watching mechanism from
          ``watchdog`` is not being used.
        :param force_polling: (default: false) True if the data source should implement auto-update via
          polling the filesystem even if a native mechanism is available. This is mainly for SDK testing.

        :return: an object (actually a lambda) to be stored in the ``update_processor_class`` configuration property
        """
        return lambda config, store, ready: _FileDataSource(store, config.data_source_update_sink, ready, paths, auto_update, poll_interval, force_polling)
