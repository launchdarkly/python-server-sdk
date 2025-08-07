"""
This module houses FDv2 types and implementations of synchronizers and
initializers for the datasystem.

All types and implementations in this module are considered internal
and are not part of the public API of the LaunchDarkly Python SDK.
They are subject to change without notice and should not be used directly
by users of the SDK.

You have been warned.
"""

from .polling import PollingResult, Requester

__all__: list[str] = ["PollingResult", "Requester"]
