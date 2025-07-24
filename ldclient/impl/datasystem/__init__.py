"""
This package contains the generic interfaces used for the data system (v1 and
v2), as well as types for v1 and v2 specific protocols.
"""

from abc import abstractmethod
from typing import Protocol

from ldclient.impl.util import Result


class Synchronizer(Protocol):
    """
    Represents a component capable of obtaining a Basis and subsequent delta
    updates asynchronously.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the initializer."""
        raise NotImplementedError

    # TODO(fdv2): Need sync method

    def close(self):
        """
        Close the synchronizer, releasing any resources it holds.
        """


class Initializer(Protocol):
    """
    Represents a component capable of obtaining a Basis via a synchronous call.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the initializer."""
        raise NotImplementedError

    @abstractmethod
    def fetch(self) -> Result:
        """
        Fetch returns a Basis, or an error if the Basis could not be retrieved.
        """
        raise NotImplementedError


__all__: list[str] = ["Synchronizer", "Initializer"]
