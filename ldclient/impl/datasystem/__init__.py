"""
This package contains the generic interfaces used for the data system (v1 and
v2), as well as types for v1 and v2 specific protocols.
"""

from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from threading import Event
from typing import Generator, Optional, Protocol

from ldclient.impl.datasystem.protocolv2 import Basis, ChangeSet
from ldclient.impl.util import _Result
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceState,
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FlagTracker
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


BasisResult = _Result[Basis, str]


class Initializer(Protocol):  # pylint: disable=too-few-public-methods
    """
    Initializer represents a component capable of retrieving a single data
    result, such as from the LD polling API.

    The intent of initializers is to quickly fetch an initial set of data,
    which may be stale but is fast to retrieve. This initial data serves as a
    foundation for a Synchronizer to build upon, enabling it to provide updates
    as new changes occur.
    """

    @abstractmethod
    def fetch(self) -> BasisResult:
        """
        fetch should retrieve the initial data set for the data source, returning
        a Basis object on success, or an error message on failure.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Update:
    """
    Update represents the results of a synchronizer's ongoing sync
    method.
    """

    state: DataSourceState
    change_set: Optional[ChangeSet] = None
    error: Optional[DataSourceErrorInfo] = None
    revert_to_fdv1: bool = False
    environment_id: Optional[str] = None


class Synchronizer(Protocol):  # pylint: disable=too-few-public-methods
    """
    Synchronizer represents a component capable of synchronizing data from an external
    data source, such as a streaming or polling API.

    It is responsible for yielding Update objects that represent the current state
    of the data source, including any changes that have occurred since the last
    synchronization.
    """

    @abstractmethod
    def sync(self) -> Generator[Update, None, None]:
        """
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
        """
        raise NotImplementedError
