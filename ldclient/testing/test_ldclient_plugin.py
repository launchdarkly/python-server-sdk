import threading
import unittest
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import patch

from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.context import Context
from ldclient.hook import (
    EvaluationDetail,
    EvaluationSeriesContext,
    Hook,
    Metadata
)
from ldclient.plugin import EnvironmentMetadata, Plugin, PluginMetadata


class ThreadSafeCounter:
    """Thread-safe counter for tracking hook execution order."""

    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def get_and_increment(self) -> int:
        """Atomically get the current value and increment it."""
        with self._lock:
            current = self._value
            self._value += 1
            return current


class ConfigurableTestHook(Hook):
    """Configurable test hook that can be customized with lambda functions for before/after evaluation."""

    def __init__(self, name: str = "Configurable Test Hook", before_evaluation_behavior=None, after_evaluation_behavior=None):
        self._name = name
        self.before_called = False
        self.after_called = False
        self.execution_order = -1
        self._state: Dict[str, Any] = {}
        self._before_evaluation_behavior = before_evaluation_behavior
        self._after_evaluation_behavior = after_evaluation_behavior

    @property
    def metadata(self) -> Metadata:
        return Metadata(name=self._name)

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        self.before_called = True
        if self._before_evaluation_behavior:
            return self._before_evaluation_behavior(self, series_context, data)
        return data

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        self.after_called = True
        if self._after_evaluation_behavior:
            return self._after_evaluation_behavior(self, series_context, data, detail)
        return data

    def set_state(self, key: str, value: Any) -> None:
        self._state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)


class ConfigurableTestPlugin(Plugin):
    """Configurable test plugin that can be customized with lambda functions for different test scenarios."""

    def __init__(self,
                 name: str = "Configurable Test Plugin",
                 hooks: Optional[List[Hook]] = None,
                 register_behavior: Optional[Callable[[Any, EnvironmentMetadata], None]] = None,
                 get_hooks_behavior: Optional[Callable[[EnvironmentMetadata], List[Hook]]] = None):
        self._name = name
        self._hooks = hooks if hooks is not None else []
        self._register_behavior = register_behavior
        self._get_hooks_behavior = get_hooks_behavior

        # State tracking
        self.registered = False
        self.registration_metadata: Optional[EnvironmentMetadata] = None
        self.registration_client: Optional[Any] = None
        self.hooks_called = False
        self.hooks_metadata: Optional[EnvironmentMetadata] = None

    @property
    def metadata(self) -> PluginMetadata:
        return PluginMetadata(name=self._name)

    def register(self, client: Any, metadata: EnvironmentMetadata) -> None:
        self.registration_client = client
        self.registration_metadata = metadata

        if self._register_behavior:
            self._register_behavior(client, metadata)

        # Only mark as registered if no exception was thrown
        self.registered = True

    def get_hooks(self, metadata: EnvironmentMetadata) -> List[Hook]:
        self.hooks_called = True
        self.hooks_metadata = metadata

        if self._get_hooks_behavior:
            return self._get_hooks_behavior(metadata)

        return self._hooks


class TestLDClientPlugin(unittest.TestCase):
    """Test cases for LDClient plugin functionality."""

    def test_plugin_environment_metadata(self):
        """Test that plugins receive correct environment metadata."""
        plugin = ConfigurableTestPlugin("Test Plugin")

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            wrapper_name="TestWrapper",
            wrapper_version="1.0.0",
            application={"id": "test-app", "version": "1.0.0"},
            plugins=[plugin]
        )

        with LDClient(config=config) as client:
            self.assertTrue(plugin.registered)
            self.assertIsNotNone(plugin.registration_metadata)

            # Verify SDK metadata
            if plugin.registration_metadata:
                self.assertEqual(plugin.registration_metadata.sdk.name, "python-server-sdk")
                self.assertEqual(plugin.registration_metadata.sdk.wrapper_name, "TestWrapper")
                self.assertEqual(plugin.registration_metadata.sdk.wrapper_version, "1.0.0")
                self.assertRegex(plugin.registration_metadata.sdk.version, r"^\d+\.\d+\.\d+$")

                # Verify application metadata
                if plugin.registration_metadata.application:
                    self.assertEqual(plugin.registration_metadata.application.id, "test-app")
                    self.assertEqual(plugin.registration_metadata.application.version, "1.0.0")

                # Verify SDK key
                self.assertEqual(plugin.registration_metadata.sdk_key, "test-sdk-key")

    def test_registers_plugins_and_executes_hooks(self):
        """Test that plugins are registered and hooks are executed."""
        hook1 = ConfigurableTestHook("Hook 1")
        hook2 = ConfigurableTestHook("Hook 2")

        plugin1 = ConfigurableTestPlugin("Plugin 1", hooks=[hook1])
        plugin2 = ConfigurableTestPlugin("Plugin 2", hooks=[hook2])

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            plugins=[plugin1, plugin2]
        )

        with LDClient(config=config) as client:
            # Verify hooks were collected
            self.assertTrue(plugin1.hooks_called)
            self.assertTrue(plugin2.hooks_called)
            self.assertTrue(plugin1.registered)
            self.assertTrue(plugin2.registered)

            # Test that hooks are called during evaluation
            client.variation("test-flag", Context.builder("user-key").build(), "default")

            # Verify hooks were called
            self.assertTrue(hook1.before_called)
            self.assertTrue(hook1.after_called)
            self.assertTrue(hook2.before_called)
            self.assertTrue(hook2.after_called)

    def test_plugin_error_handling_get_hooks(self):
        """Test that errors get_hooks are handled gracefully."""
        error_plugin = ConfigurableTestPlugin(
            "Error Plugin",
            get_hooks_behavior=lambda metadata: (_ for _ in ()).throw(Exception("Get hooks error in Error Plugin"))
        )
        normal_hook = ConfigurableTestHook("Normal Hook")
        normal_plugin = ConfigurableTestPlugin("Normal Plugin", hooks=[normal_hook])

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            plugins=[error_plugin, normal_plugin]
        )

        # The hooks cannot be accessed, but the plugin will still get registered.
        with patch('ldclient.impl.util.log.error') as mock_log_error:
            with LDClient(config=config) as client:
                self.assertTrue(normal_plugin.registered)
                self.assertTrue(error_plugin.registered)

                client.variation("test-flag", Context.builder("user-key").build(), "default")

                self.assertTrue(normal_hook.before_called)
                self.assertTrue(normal_hook.after_called)

                # Verify that the error was logged with the correct message
                mock_log_error.assert_called_once()
                # Check the format string and arguments separately
                format_string = mock_log_error.call_args[0][0]
                format_args = mock_log_error.call_args[0][1:]
                self.assertEqual(format_string, "Error getting hooks from plugin %s: %s")
                self.assertEqual(len(format_args), 2)
                self.assertEqual(format_args[0], "Error Plugin")
                self.assertIn("Get hooks error in Error Plugin", str(format_args[1]))

    def test_plugin_error_handling_register(self):
        """Test that errors during plugin registration are handled gracefully."""
        error_plugin = ConfigurableTestPlugin(
            "Error Plugin",
            register_behavior=lambda client, metadata: (_ for _ in ()).throw(Exception("Registration error in Error Plugin"))
        )
        normal_hook = ConfigurableTestHook("Normal Hook")
        normal_plugin = ConfigurableTestPlugin("Normal Plugin", hooks=[normal_hook])

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            plugins=[error_plugin, normal_plugin]
        )

        # Should not raise an exception
        with patch('ldclient.impl.util.log.error') as mock_log_error:
            with LDClient(config=config) as client:
                # Normal plugin should still be registered
                self.assertTrue(normal_plugin.registered)

                # Error plugin should not be registered
                self.assertFalse(error_plugin.registered)

                client.variation("test-flag", Context.builder("user-key").build(), "default")

                self.assertTrue(normal_hook.before_called)
                self.assertTrue(normal_hook.after_called)

                # Verify that the error was logged with the correct message
                mock_log_error.assert_called_once()
                # Check the format string and arguments separately
                format_string = mock_log_error.call_args[0][0]
                format_args = mock_log_error.call_args[0][1:]
                self.assertEqual(format_string, "Error registering plugin %s: %s")
                self.assertEqual(len(format_args), 2)
                self.assertEqual(format_args[0], "Error Plugin")
                self.assertIn("Registration error in Error Plugin", str(format_args[1]))

    def test_plugin_with_existing_hooks(self):
        """Test that plugin hooks work alongside existing hooks and config hooks are called before plugin hooks."""
        counter = ThreadSafeCounter()

        def make_ordered_before(counter):
            return lambda hook, series_context, data: (
                setattr(hook, 'execution_order', counter.get_and_increment()) or data
            )
        existing_hook = ConfigurableTestHook("Existing Hook", before_evaluation_behavior=make_ordered_before(counter))
        plugin_hook = ConfigurableTestHook("Plugin Hook", before_evaluation_behavior=make_ordered_before(counter))

        plugin = ConfigurableTestPlugin("Test Plugin", hooks=[plugin_hook])

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            hooks=[existing_hook],
            plugins=[plugin]
        )

        with LDClient(config=config) as client:
            # Test that both hooks are called
            client.variation("test-flag", Context.builder("user-key").build(), "default")

            # Verify hooks were called
            self.assertTrue(existing_hook.before_called)
            self.assertTrue(existing_hook.after_called)
            self.assertTrue(plugin_hook.before_called)
            self.assertTrue(plugin_hook.after_called)

            # Verify that config hooks are called before plugin hooks
            self.assertLess(existing_hook.execution_order, plugin_hook.execution_order,
                            "Config hooks should be called before plugin hooks")

    def test_plugin_no_hooks(self):
        """Test that plugins without hooks work correctly."""
        plugin = ConfigurableTestPlugin("No Hooks Plugin", hooks=[])

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            plugins=[plugin]
        )

        with LDClient(config=config) as client:
            self.assertTrue(plugin.registered)
            self.assertTrue(plugin.hooks_called)

            # Should work normally without hooks
            result = client.variation("test-flag", Context.builder("user-key").build(), False)
            self.assertEqual(result, False)

    def test_plugin_client_access(self):
        """Test that plugins can access the client during registration and their hooks are called."""
        hook = ConfigurableTestHook("Client Access Hook")

        def register_behavior(client, metadata):
            # Call variation during registration to test that hooks are available
            # This should trigger the plugin's hook
            result = client.variation("test-flag", Context.builder("user-key").build(), "default")
            # Store whether the hook was called during registration
            hook.set_state("called_during_registration", hook.before_called)

        plugin = ConfigurableTestPlugin(
            "Client Access Plugin",
            hooks=[hook],
            register_behavior=register_behavior
        )

        config = Config(
            sdk_key="test-sdk-key",
            send_events=False,
            offline=True,
            plugins=[plugin]
        )

        with LDClient(config=config) as client:
            self.assertTrue(plugin.registered)
            self.assertIs(plugin.registration_client, client)

            # Verify that the plugin's hook was called when it called variation during registration
            self.assertTrue(hook.get_state("called_during_registration", False),
                            "Plugin's hook should be called when variation is called during registration")
            self.assertTrue(hook.before_called)
            self.assertTrue(hook.after_called)
