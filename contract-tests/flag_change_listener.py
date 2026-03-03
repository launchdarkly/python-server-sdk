import logging
import threading
from typing import Callable, Dict

import requests

from ldclient.context import Context
from ldclient.interfaces import FlagChange, FlagTracker, FlagValueChange

log = logging.getLogger('testservice')


class ListenerRegistry:
    """Manages all active flag change listener registrations for a single SDK client entity."""

    def __init__(self, tracker: FlagTracker):
        self._tracker = tracker
        self._lock = threading.Lock()
        # Maps listener_id -> (sdk_listener callable, cleanup function)
        self._listeners: Dict[str, Callable] = {}

    def register_flag_change_listener(self, listener_id: str, callback_uri: str):
        """Register a general flag change listener that fires on any flag configuration change."""
        def on_flag_change(flag_change: FlagChange):
            payload = {
                'listenerId': listener_id,
                'flagKey': flag_change.key,
            }
            try:
                requests.post(callback_uri, json=payload)
            except Exception as e:
                log.warning('Failed to post flag change notification: %s', e)

        with self._lock:
            # If a listener with this ID already exists, unregister the old one first
            if listener_id in self._listeners:
                self._tracker.remove_listener(self._listeners[listener_id])

            self._tracker.add_listener(on_flag_change)
            self._listeners[listener_id] = on_flag_change

    def register_flag_value_change_listener(
        self,
        listener_id: str,
        flag_key: str,
        context: Context,
        default_value,
        callback_uri: str,
    ):
        """Register a flag value change listener that fires when the evaluated value changes."""
        def on_value_change(change: FlagValueChange):
            payload = {
                'listenerId': listener_id,
                'flagKey': change.key,
                'oldValue': change.old_value,
                'newValue': change.new_value,
            }
            try:
                requests.post(callback_uri, json=payload)
            except Exception as e:
                log.warning('Failed to post flag value change notification: %s', e)

        # add_flag_value_change_listener returns the underlying listener
        # that must be passed to remove_listener to unsubscribe
        with self._lock:
            if listener_id in self._listeners:
                self._tracker.remove_listener(self._listeners[listener_id])

            underlying_listener = self._tracker.add_flag_value_change_listener(flag_key, context, on_value_change)
            self._listeners[listener_id] = underlying_listener

    def unregister(self, listener_id: str) -> bool:
        """Unregister a previously registered listener. Returns False if not found."""
        with self._lock:
            listener = self._listeners.pop(listener_id, None)

        if listener is None:
            return False

        self._tracker.remove_listener(listener)
        return True

    def close_all(self):
        """Unregister all listeners. Called when the SDK client entity shuts down."""
        with self._lock:
            listeners = dict(self._listeners)
            self._listeners.clear()

        for listener in listeners.values():
            self._tracker.remove_listener(listener)
