"""
This package contains the generic interfaces used for the data system (v1 and
v2), as well as types for v1 and v2 specific protocols.
"""

from abc import abstractmethod
from enum import Enum
from threading import Event
from typing import Protocol, runtime_checkable

from ldclient.interfaces import (
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FlagTracker,
    ReadOnlyStore
)


class DataAvailability(str, Enum):
    """
    Represents the availability of data in the SDK.
    """

    DEFAULTS = "defaults"
    """
    The SDK has no data and will evaluate flags using the application-provided default values.
    """

    CACHED = "cached"
    """
    The SDK has data, not necessarily the latest, which will be used to evaluate flags.
    """

    REFRESHED = "refreshed"
    """
    The SDK has obtained, at least once, the latest known data from LaunchDarkly.
    """

    def at_least(self, other: "DataAvailability") -> bool:
        """
        Returns whether this availability level is **at least** as good as the other.
        """
        if self == other:
            return True

        if self == DataAvailability.REFRESHED:
            return True

        if self == DataAvailability.CACHED and other == DataAvailability.DEFAULTS:
            return True

        return False


class DataSystem(Protocol):
    """
    Represents the requirements the client has for storing/retrieving/detecting changes related
    to the SDK's data model.
    """

    @abstractmethod
    def start(self, set_on_ready: Event):
        """
        Starts the data system.

        This method will return immediately. The provided `Event` will be set when the system
        has reached an initial state (either permanently failed, e.g. due to bad auth, or
        succeeded)
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """
        Halts the data system. Should be called when the client is closed to stop any long running
        operations.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        """
        Returns an interface for tracking the status of the data source.

        The data source is the mechanism that the SDK uses to get feature flag configurations, such
        as a streaming connection (the default) or poll requests. The
        :class:`ldclient.interfaces.DataSourceStatusProvider` has methods for checking whether the
        data source is (as far as the SDK knows) currently operational and tracking changes in this
        status.

        :return: The data source status provider
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        """
        Returns an interface for tracking the status of a persistent data store.

        The provider has methods for checking whether the data store is (as far
        as the SDK knows) currently operational, tracking changes in this
        status, and getting cache statistics. These are only relevant for a
        persistent data store; if you are using an in-memory data store, then
        this method will return a stub object that provides no information.

        :return: The data store status provider
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def flag_tracker(self) -> FlagTracker:
        """
        Returns an interface for tracking changes in feature flag configurations.

        The :class:`ldclient.interfaces.FlagTracker` contains methods for
        requesting notifications about feature flag changes using an event
        listener model.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def data_availability(self) -> DataAvailability:
        """
        Indicates what form of data is currently available.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def target_availability(self) -> DataAvailability:
        """
        Indicates the ideal form of data attainable given the current configuration.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def store(self) -> ReadOnlyStore:
        """
        Returns the data store used by the data system.
        """
        raise NotImplementedError


class DiagnosticAccumulator(Protocol):
    def record_stream_init(self, timestamp, duration, failed):
        raise NotImplementedError

    def record_events_in_batch(self, events_in_batch):
        raise NotImplementedError

    def create_event_and_reset(self, dropped_events, deduplicated_users):
        raise NotImplementedError


@runtime_checkable
class DiagnosticSource(Protocol):
    @abstractmethod
    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Set the diagnostic_accumulator to be used for reporting diagnostic events.
        """
        raise NotImplementedError
