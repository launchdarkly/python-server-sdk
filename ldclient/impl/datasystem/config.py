"""
Configuration for LaunchDarkly's data acquisition strategy.
"""

from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar

from ldclient.config import Config as LDConfig
from ldclient.impl.datasourcev2.polling import (
    PollingDataSource,
    PollingDataSourceBuilder,
    Urllib3PollingRequester
)
from ldclient.impl.datasourcev2.streaming import (
    StreamingDataSource,
    StreamingDataSourceBuilder
)
from ldclient.impl.datasystem import Initializer, Synchronizer

T = TypeVar("T")

Builder = Callable[[], T]


@dataclass(frozen=True)
class Config:
    """
    Configuration for LaunchDarkly's data acquisition strategy.
    """

    initializers: Optional[List[Builder[Initializer]]]
    """The initializers for the data system."""

    primary_synchronizer: Builder[Synchronizer]
    """The primary synchronizer for the data system."""

    secondary_synchronizer: Optional[Builder[Synchronizer]]
    """The secondary synchronizers for the data system."""


class ConfigBuilder:  # pylint: disable=too-few-public-methods
    """
    Builder for the data system configuration.
    """

    _initializers: Optional[List[Builder[Initializer]]] = None
    _primary_synchronizer: Optional[Builder[Synchronizer]] = None
    _secondary_synchronizer: Optional[Builder[Synchronizer]] = None

    def initializers(self, initializers: List[Builder[Initializer]]) -> "ConfigBuilder":
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

    def build(self) -> Config:
        """
        Builds the data system configuration.
        """
        if self._primary_synchronizer is None:
            raise ValueError("Primary synchronizer must be set")

        return Config(
            initializers=self._initializers,
            primary_synchronizer=self._primary_synchronizer,
            secondary_synchronizer=self._secondary_synchronizer,
        )


def __polling_ds_builder(config: LDConfig) -> Builder[PollingDataSource]:
    def builder() -> PollingDataSource:
        requester = Urllib3PollingRequester(config)
        polling_ds = PollingDataSourceBuilder(config)
        polling_ds.requester(requester)

        return polling_ds.build()

    return builder


def __streaming_ds_builder(config: LDConfig) -> Builder[StreamingDataSource]:
    def builder() -> StreamingDataSource:
        return StreamingDataSourceBuilder(config).build()

    return builder


def default(config: LDConfig) -> ConfigBuilder:
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

    polling_builder = __polling_ds_builder(config)
    streaming_builder = __streaming_ds_builder(config)

    builder = ConfigBuilder()
    builder.initializers([polling_builder])
    builder.synchronizers(streaming_builder, polling_builder)

    return builder


def streaming(config: LDConfig) -> ConfigBuilder:
    """
    Streaming configures the SDK to efficiently streams flag/segment data
    in the background, allowing evaluations to operate on the latest data
    with no additional latency.
    """

    streaming_builder = __streaming_ds_builder(config)

    builder = ConfigBuilder()
    builder.synchronizers(streaming_builder)

    return builder


def polling(config: LDConfig) -> ConfigBuilder:
    """
    Polling configures the SDK to regularly poll an endpoint for
    flag/segment data in the background. This is less efficient than
    streaming, but may be necessary in some network environments.
    """

    polling_builder = __polling_ds_builder(config)

    builder = ConfigBuilder()
    builder.synchronizers(polling_builder)

    return builder


def custom() -> ConfigBuilder:
    """
    Custom returns a builder suitable for creating a custom data
    acquisition strategy. You may configure how the SDK uses a Persistent
    Store, how the SDK obtains an initial set of data, and how the SDK
    keeps data up-to-date.
    """

    return ConfigBuilder()


# TODO(fdv2): Implement these methods
#
# Daemon configures the SDK to read from a persistent store integration
# that is populated by Relay Proxy or other SDKs. The SDK will not connect
# to LaunchDarkly. In this mode, the SDK never writes to the data store.

# PersistentStore is similar to Default, with the addition of a persistent
# store integration. Before data has arrived from LaunchDarkly, the SDK is
# able to evaluate flags using data from the persistent store. Once fresh
# data is available, the SDK will no longer read from the persistent store,
# although it will keep it up-to-date.

# WithEndpoints configures the data system with custom endpoints for
# LaunchDarkly's streaming and polling synchronizers. This method is not
# necessary for most use-cases, but can be useful for testing or custom
# network configurations.
#
# Any endpoint that is not specified (empty string) will be treated as the
# default LaunchDarkly SaaS endpoint for that service.

# WithRelayProxyEndpoints configures the data system with a single endpoint
# for LaunchDarkly's streaming and polling synchronizers. The endpoint
# should be Relay Proxy's base URI, for example http://localhost:8123.
