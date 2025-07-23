"""
Tests for the plugin interface.
"""

import unittest
from typing import Any, List, Optional
from unittest.mock import Mock

from ldclient.config import Config
from ldclient.hook import (
    EvaluationDetail,
    EvaluationSeriesContext,
    Hook,
    Metadata
)
from ldclient.plugin import (
    ApplicationMetadata,
    EnvironmentMetadata,
    Plugin,
    PluginMetadata,
    SdkMetadata
)


class ExampleHook(Hook):
    """Example hook implementation for the example plugin."""

    @property
    def metadata(self) -> Metadata:
        return Metadata(name="Example Plugin Hook")

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        """Called before flag evaluation."""
        # Add some data to track in the evaluation series
        data['example_plugin_before'] = True
        return data

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        """Called after flag evaluation."""
        # Add some data to track in the evaluation series
        data['example_plugin_after'] = True
        return data


class ExamplePlugin(Plugin):
    """
    Example plugin implementation.

    This plugin demonstrates how to implement the plugin interface by:
    1. Providing metadata about the plugin
    2. Registering with the client
    3. Providing hooks for SDK observation
    """

    def __init__(self, name: str = "Example Plugin"):
        self._name = name
        self._client = None
        self._environment_metadata: Optional[EnvironmentMetadata] = None

    @property
    def metadata(self) -> PluginMetadata:
        """Get metadata about the plugin implementation."""
        return PluginMetadata(name=self._name)

    def register(self, client: Any, metadata: EnvironmentMetadata) -> None:
        """
        Register the plugin with the SDK client.

        This method is called during SDK initialization to allow the plugin
        to set up any necessary integrations, register hooks, or perform
        other initialization tasks.
        """
        self._client = client
        self._environment_metadata = metadata

        # Example: Log some information about the environment
        print(f"Example Plugin registered with SDK {metadata.sdk.name} version {metadata.sdk.version}")
        if metadata.application:
            print(f"Application: {metadata.application.id} version {metadata.application.version}")

    def get_hooks(self, metadata: EnvironmentMetadata) -> List[Hook]:
        """
        Get a list of hooks that this plugin provides.

        This method is called before register() to collect all hooks from
        plugins. The hooks returned will be added to the SDK's hook configuration.
        """
        return [ExampleHook()]


class TestPlugin(unittest.TestCase):
    """Test cases for the plugin interface."""

    def test_plugin_metadata(self):
        """Test that plugin metadata is correctly structured."""
        metadata = PluginMetadata(name="Test Plugin")
        self.assertEqual(metadata.name, "Test Plugin")

    def test_environment_metadata(self):
        """Test that environment metadata is correctly structured."""
        sdk_metadata = SdkMetadata(name="test-sdk", version="1.0.0")
        app_metadata = ApplicationMetadata(id="test-app", version="1.0.0")

        env_metadata = EnvironmentMetadata(
            sdk=sdk_metadata,
            application=app_metadata,
            sdk_key="test-key"
        )

        self.assertEqual(env_metadata.sdk.name, "test-sdk")
        self.assertEqual(env_metadata.sdk.version, "1.0.0")
        if env_metadata.application:
            self.assertEqual(env_metadata.application.id, "test-app")
            self.assertEqual(env_metadata.application.version, "1.0.0")
        self.assertEqual(env_metadata.sdk_key, "test-key")

    def test_example_plugin(self):
        """Test that the example plugin works correctly."""
        plugin = ExamplePlugin("Test Example Plugin")

        # Test metadata
        metadata = plugin.metadata
        self.assertEqual(metadata.name, "Test Example Plugin")

        # Test hooks
        sdk_metadata = SdkMetadata(name="test-sdk", version="1.0.0")
        env_metadata = EnvironmentMetadata(sdk=sdk_metadata, sdk_key="test-key")

        hooks = plugin.get_hooks(env_metadata)
        self.assertEqual(len(hooks), 1)
        self.assertIsInstance(hooks[0], Hook)

        # Test registration
        mock_client = Mock()
        plugin.register(mock_client, env_metadata)
        self.assertEqual(plugin._client, mock_client)
        self.assertEqual(plugin._environment_metadata, env_metadata)

    def test_config_with_plugins(self):
        """Test that Config can be created with plugins."""
        plugin = ExamplePlugin()
        config = Config(sdk_key="test-key", plugins=[plugin])

        self.assertEqual(len(config.plugins), 1)
        self.assertEqual(config.plugins[0], plugin)

    def test_config_without_plugins(self):
        """Test that Config works without plugins."""
        config = Config(sdk_key="test-key")
        self.assertEqual(len(config.plugins), 0)


if __name__ == '__main__':
    unittest.main()
