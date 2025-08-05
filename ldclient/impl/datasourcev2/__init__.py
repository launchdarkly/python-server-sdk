"""
This module houses FDv2 types and implementations of synchronizers and
initializers for the datasystem.

All types and implementations in this module are considered internal
and are not part of the public API of the LaunchDarkly Python SDK.
They are subject to change without notice and should not be used directly
by users of the SDK.

You have been warned.
"""

from abc import abstractmethod
from dataclasses import dataclass
from typing import Generator, Mapping, Optional, Protocol, Tuple

from ldclient.impl.datasystem.protocolv2 import Basis, ChangeSet
from ldclient.impl.util import _Result
from ldclient.interfaces import DataSourceErrorInfo, DataSourceState

PollingResult = _Result[Tuple[ChangeSet, Mapping], str]


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
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.
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


__all__: list[str] = [
    # Initializer-related types
    "BasisResult",
    "Initializer",
    # Synchronizer-related types
    "Update",
    "Synchronizer",
]
