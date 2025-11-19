"""
Configuration for LaunchDarkly's data acquisition strategy.
"""

from typing import Callable, List, Optional, TypeVar

from ldclient.config import Config as LDConfig
from ldclient.config import DataSystemConfig
from ldclient.impl.datasourcev2.polling import (
    PollingDataSource,
    PollingDataSourceBuilder,
    Urllib3FDv1PollingRequester,
    Urllib3PollingRequester
)
from ldclient.impl.datasourcev2.streaming import (
    StreamingDataSource,
    StreamingDataSourceBuilder
)
from ldclient.interfaces import (
    DataStoreMode,
    FeatureStore,
    Initializer,
    Synchronizer
)

T = TypeVar("T")

Builder = Callable[[LDConfig], T]


class ConfigBuilder:  # pylint: disable=too-few-public-methods
    """
    Builder for the data system configuration.
    """

    def __init__(self) -> None:
        self._initializers: Optional[List[Builder[Initializer]]] = None
        self._primary_synchronizer: Optional[Builder[Synchronizer]] = None
        self._secondary_synchronizer: Optional[Builder[Synchronizer]] = None
        self._fdv1_fallback_synchronizer: Optional[Builder[Synchronizer]] = None
        self._store_mode: DataStoreMode = DataStoreMode.READ_ONLY
        self._data_store: Optional[FeatureStore] = None

    def initializers(self, initializers: Optional[List[Builder[Initializer]]]) -> "ConfigBuilder":
        """
        Sets the initializers for the data system.
        """
        self._initializers = initializers
        return self

    def synchronizers(
        self,
        primary: Builder[Synchronizer],
        secondary: Optional[Builder[Synchronizer]] = None,
    ) -> "ConfigBuilder":
        """
        Sets the synchronizers for the data system.
        """
        self._primary_synchronizer = primary
        self._secondary_synchronizer = secondary
        return self

    def fdv1_compatible_synchronizer(
            self,
            fallback: Builder[Synchronizer]
    ) -> "ConfigBuilder":
        """
        Configures the SDK with a fallback synchronizer that is compatible with
        the Flag Delivery v1 API.
        """
        self._fdv1_fallback_synchronizer = fallback
        return self

    def data_store(self, data_store: FeatureStore, store_mode: DataStoreMode) -> "ConfigBuilder":
        """
        Sets the data store configuration for the data system.
        """
        self._data_store = data_store
        self._store_mode = store_mode
        return self

    def build(self) -> DataSystemConfig:
        """
        Builds the data system configuration.
        """
        if self._secondary_synchronizer is not None and self._primary_synchronizer is None:
            raise ValueError("Primary synchronizer must be set if secondary is set")

        return DataSystemConfig(
            initializers=self._initializers,
            primary_synchronizer=self._primary_synchronizer,
            secondary_synchronizer=self._secondary_synchronizer,
            fdv1_fallback_synchronizer=self._fdv1_fallback_synchronizer,
            data_store_mode=self._store_mode,
            data_store=self._data_store,
        )


def __polling_ds_builder() -> Builder[PollingDataSource]:
    def builder(config: LDConfig) -> PollingDataSource:
        requester = Urllib3PollingRequester(config)
        polling_ds = PollingDataSourceBuilder(config)
        polling_ds.requester(requester)

        return polling_ds.build()

    return builder


def __fdv1_fallback_ds_builder() -> Builder[PollingDataSource]:
    def builder(config: LDConfig) -> PollingDataSource:
        requester = Urllib3FDv1PollingRequester(config)
        polling_ds = PollingDataSourceBuilder(config)
        polling_ds.requester(requester)

        return polling_ds.build()

    return builder


def __streaming_ds_builder() -> Builder[StreamingDataSource]:
    def builder(config: LDConfig) -> StreamingDataSource:
        return StreamingDataSourceBuilder(config).build()

    return builder


def default() -> ConfigBuilder:
    """
    Default is LaunchDarkly's recommended flag data acquisition strategy.

    Currently, it operates a two-phase method for obtaining data: first, it
    requests data from LaunchDarkly's global CDN. Then, it initiates a
    streaming connection to LaunchDarkly's Flag Delivery services to
    receive real-time updates.

    If the streaming connection is interrupted for an extended period of
    time, the SDK will automatically fall back to polling the global CDN
    for updates.
    """

    polling_builder = __polling_ds_builder()
    streaming_builder = __streaming_ds_builder()
    fallback = __fdv1_fallback_ds_builder()

    builder = ConfigBuilder()
    builder.initializers([polling_builder])
    builder.synchronizers(streaming_builder, polling_builder)
    builder.fdv1_compatible_synchronizer(fallback)

    return builder


def streaming() -> ConfigBuilder:
    """
    Streaming configures the SDK to efficiently streams flag/segment data
    in the background, allowing evaluations to operate on the latest data
    with no additional latency.
    """

    streaming_builder = __streaming_ds_builder()
    fallback = __fdv1_fallback_ds_builder()

    builder = ConfigBuilder()
    builder.synchronizers(streaming_builder)
    builder.fdv1_compatible_synchronizer(fallback)

    return builder


def polling() -> ConfigBuilder:
    """
    Polling configures the SDK to regularly poll an endpoint for
    flag/segment data in the background. This is less efficient than
    streaming, but may be necessary in some network environments.
    """

    polling_builder: Builder[Synchronizer] = __polling_ds_builder()
    fallback = __fdv1_fallback_ds_builder()

    builder = ConfigBuilder()
    builder.synchronizers(polling_builder)
    builder.fdv1_compatible_synchronizer(fallback)

    return builder


def custom() -> ConfigBuilder:
    """
    Custom returns a builder suitable for creating a custom data
    acquisition strategy. You may configure how the SDK uses a Persistent
    Store, how the SDK obtains an initial set of data, and how the SDK
    keeps data up-to-date.
    """

    return ConfigBuilder()


def daemon(store: FeatureStore) -> ConfigBuilder:
    """
    Daemon configures the SDK to read from a persistent store integration
    that is populated by Relay Proxy or other SDKs. The SDK will not connect
    to LaunchDarkly. In this mode, the SDK never writes to the data store.
    """
    return default().data_store(store, DataStoreMode.READ_ONLY)


def persistent_store(store: FeatureStore) -> ConfigBuilder:
    """
    PersistentStore is similar to Default, with the addition of a persistent
    store integration. Before data has arrived from LaunchDarkly, the SDK is
    able to evaluate flags using data from the persistent store. Once fresh
    data is available, the SDK will no longer read from the persistent store,
    although it will keep it up-to-date.
    """
    return default().data_store(store, DataStoreMode.READ_WRITE)
