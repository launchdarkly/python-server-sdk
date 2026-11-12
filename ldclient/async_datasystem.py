"""
Configuration for the async SDK's data acquisition strategy.

This module is the async counterpart of :mod:`ldclient.datasystem`. Its functions build an
:class:`ldclient.async_config.AsyncDataSystemConfig` for
:class:`ldclient.async_client.AsyncLDClient`.

.. caution::
    This feature is experimental and should NOT be considered ready for production use. It may
    change or be removed without notice and is not subject to backwards compatibility guarantees.
"""

from typing import List, Optional

from ldclient.async_config import AsyncDataSystemConfig
from ldclient.config import DataSourceBuilder
from ldclient.impl.datasource.async_polling import (
    AsyncFallbackToFDv1PollingDataSourceBuilder,
    AsyncPollingDataSourceBuilder
)
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingDataSourceBuilder
)
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncInitializer,
    AsyncSynchronizer,
    DataStoreMode
)


class AsyncConfigBuilder:  # pylint: disable=too-few-public-methods
    """
    Builder for the async data system configuration.
    """

    def __init__(self) -> None:
        self._initializers: Optional[List[DataSourceBuilder[AsyncInitializer]]] = None
        self._synchronizers: List[DataSourceBuilder[AsyncSynchronizer]] = []
        self._fdv1_fallback_synchronizer: Optional[DataSourceBuilder[AsyncSynchronizer]] = None
        self._store_mode: DataStoreMode = DataStoreMode.READ_ONLY
        self._data_store: Optional[AsyncFeatureStore] = None

    def initializers(self, initializers: Optional[List[DataSourceBuilder[AsyncInitializer]]]) -> "AsyncConfigBuilder":
        """
        Sets the initializers for the data system.
        """
        self._initializers = initializers
        return self

    def synchronizers(self, *sync_builders: DataSourceBuilder[AsyncSynchronizer]) -> "AsyncConfigBuilder":
        """
        Sets the synchronizers for the data system.

        Accepts one or more synchronizer builders, ordered by preference. The first synchronizer is
        the most preferred. The others are fallbacks, in order of decreasing preference.
        """
        if len(sync_builders) == 0:
            raise ValueError("At least one synchronizer must be provided")
        self._synchronizers = list(sync_builders)
        return self

    def fdv1_compatible_synchronizer(self, fallback: DataSourceBuilder[AsyncSynchronizer]) -> "AsyncConfigBuilder":
        """
        Configures the SDK with a fallback synchronizer that is compatible with the Flag Delivery
        v1 API.
        """
        self._fdv1_fallback_synchronizer = fallback
        return self

    def data_store(self, data_store: AsyncFeatureStore, store_mode: DataStoreMode) -> "AsyncConfigBuilder":
        """
        Sets the async persistent data store and the mode in which the SDK uses it.
        """
        self._data_store = data_store
        self._store_mode = store_mode
        return self

    def build(self) -> AsyncDataSystemConfig:
        """
        Builds the async data system configuration.
        """
        return AsyncDataSystemConfig(
            initializers=self._initializers,
            synchronizers=self._synchronizers if len(self._synchronizers) > 0 else None,
            fdv1_fallback_synchronizer=self._fdv1_fallback_synchronizer,
            data_store_mode=self._store_mode,
            data_store=self._data_store,
        )


def polling_ds_builder() -> AsyncPollingDataSourceBuilder:
    """
    Returns a builder for an async polling data source.
    """
    return AsyncPollingDataSourceBuilder()


def fdv1_fallback_ds_builder() -> AsyncFallbackToFDv1PollingDataSourceBuilder:
    """
    Returns a builder for an async fallback polling data source that is compatible with the Flag
    Delivery v1 API.
    """
    return AsyncFallbackToFDv1PollingDataSourceBuilder()


def streaming_ds_builder() -> AsyncStreamingDataSourceBuilder:
    """
    Returns a builder for an async streaming data source.
    """
    return AsyncStreamingDataSourceBuilder()


def default() -> AsyncConfigBuilder:
    """
    Default is LaunchDarkly's recommended flag data acquisition strategy.

    The SDK first requests data from LaunchDarkly's global CDN. Then it opens a streaming
    connection to LaunchDarkly's Flag Delivery services to receive real-time updates. If the
    streaming connection is interrupted for an extended period of time, the SDK falls back to
    polling the global CDN for updates.
    """
    polling_builder = polling_ds_builder()
    streaming_builder = streaming_ds_builder()
    fallback = fdv1_fallback_ds_builder()

    builder = AsyncConfigBuilder()
    builder.initializers([polling_builder])
    builder.synchronizers(streaming_builder, polling_builder)
    builder.fdv1_compatible_synchronizer(fallback)

    return builder


def streaming() -> AsyncConfigBuilder:
    """
    Streaming configures the SDK to stream flag and segment data in the background, so that
    evaluations use the latest data with no additional latency.
    """
    builder = AsyncConfigBuilder()
    builder.synchronizers(streaming_ds_builder())
    builder.fdv1_compatible_synchronizer(fdv1_fallback_ds_builder())

    return builder


def polling() -> AsyncConfigBuilder:
    """
    Polling configures the SDK to poll an endpoint for flag and segment data at a regular
    interval. This is less efficient than streaming, but some network environments need it.
    """
    polling_builder: DataSourceBuilder[AsyncSynchronizer] = polling_ds_builder()

    builder = AsyncConfigBuilder()
    builder.synchronizers(polling_builder)
    builder.fdv1_compatible_synchronizer(fdv1_fallback_ds_builder())

    return builder


def custom() -> AsyncConfigBuilder:
    """
    Custom returns an empty builder for a custom data acquisition strategy. You configure how the
    SDK uses a persistent store, how it gets an initial set of data, and how it keeps the data up
    to date.
    """
    return AsyncConfigBuilder()


def daemon(store: AsyncFeatureStore) -> AsyncConfigBuilder:
    """
    Daemon configures the SDK to read from an async persistent store that the Relay Proxy or
    another SDK populates. The SDK does not connect to LaunchDarkly, and it never writes to the
    data store.
    """
    return custom().data_store(store, DataStoreMode.READ_ONLY)


def persistent_store(store: AsyncFeatureStore) -> AsyncConfigBuilder:
    """
    PersistentStore is the same as :func:`default`, with an async persistent store added. Before
    data arrives from LaunchDarkly, the SDK evaluates flags with data from the persistent store.
    After fresh data arrives, the SDK no longer reads from the persistent store, but it keeps the
    store up to date.
    """
    return default().data_store(store, DataStoreMode.READ_WRITE)
