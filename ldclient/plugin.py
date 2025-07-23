from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail, FeatureFlagsState
from ldclient.hook import Hook
from ldclient.impl import AnyNum
from ldclient.impl.evaluator import error_reason
from ldclient.interfaces import (
    BigSegmentStoreStatusProvider,
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FlagTracker
)

if TYPE_CHECKING:
    from ldclient.client import LDClient


@dataclass
class SdkMetadata:
    """
    Metadata about the SDK.
    """
    name: str  #: The id of the SDK (e.g., "python-server-sdk")
    version: str  #: The version of the SDK
    wrapper_name: Optional[str] = None  #: The wrapper name if this SDK is a wrapper
    wrapper_version: Optional[str] = None  #: The wrapper version if this SDK is a wrapper


@dataclass
class ApplicationMetadata:
    """
    Metadata about the application using the SDK.
    """
    id: Optional[str] = None  #: The id of the application
    version: Optional[str] = None  #: The version of the application


@dataclass
class EnvironmentMetadata:
    """
    Metadata about the environment in which the SDK is running.
    """
    sdk: SdkMetadata  #: Information about the SDK
    sdk_key: Optional[str] = None  #: The SDK key used to initialize the SDK
    application: Optional[ApplicationMetadata] = None  #: Information about the application


@dataclass
class PluginMetadata:
    """
    Metadata about a plugin implementation.
    """
    name: str  #: A name representing the plugin instance


class Plugin:
    """
    Abstract base class for extending SDK functionality via plugins.

    All provided plugin implementations **MUST** inherit from this class.

    This class includes default implementations for optional methods. This
    allows LaunchDarkly to expand the list of plugin methods without breaking
    customer integrations.

    Plugins provide an interface which allows for initialization, access to
    credentials, and hook registration in a single interface.
    """

    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def metadata(self) -> PluginMetadata:
        """
        Get metadata about the plugin implementation.

        :return: Metadata containing information about the plugin
        """
        return PluginMetadata(name='UNDEFINED')

    @abstractmethod
    def register(self, client: LDClient, metadata: EnvironmentMetadata) -> None:
        """
        Register the plugin with the SDK client.

        This method is called during SDK initialization to allow the plugin
        to set up any necessary integrations, register hooks, or perform
        other initialization tasks.

        :param client: The LDClient instance
        :param metadata: Metadata about the environment in which the SDK is running
        """
        pass

    @abstractmethod
    def get_hooks(self, metadata: EnvironmentMetadata) -> List[Hook]:
        """
        Get a list of hooks that this plugin provides.

        This method is called before register() to collect all hooks from
        plugins. The hooks returned will be added to the SDK's hook configuration.

        :param metadata: Metadata about the environment in which the SDK is running
        :return: A list of hooks to be registered with the SDK
        """
        return []
