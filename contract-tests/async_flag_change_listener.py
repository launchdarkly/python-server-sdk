import asyncio
import logging
from typing import Any, Dict

from ldclient import Context

global_log = logging.getLogger('async_testservice')


class AsyncListenerRegistry:
    """Manages flag change listener registrations for a single AsyncLDClient entity."""

    def __init__(self, tracker):
        self._tracker = tracker
        self._lock = asyncio.Lock()
        # Maps listener_id -> underlying listener (sync callable or AsyncFlagValueChangeListener)
        self._listeners: Dict[str, Any] = {}

    async def register_flag_change_listener(self, listener_id: str, callback_uri: str):
        import aiohttp

        async def on_flag_change(flag_change):
            payload = {
                'listenerId': listener_id,
                'flagKey': flag_change.key,
            }
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(callback_uri, json=payload)
            except Exception as e:
                global_log.warning('Failed to post flag change notification: %s', e)

        # AsyncFlagTrackerImpl.add_listener takes a sync callable.
        # We schedule the coroutine via run_coroutine_threadsafe so the sync wrapper remains non-blocking.
        loop = asyncio.get_running_loop()

        def sync_wrapper(flag_change):
            asyncio.run_coroutine_threadsafe(on_flag_change(flag_change), loop)

        async with self._lock:
            if listener_id in self._listeners:
                self._tracker.remove_listener(self._listeners[listener_id])
            self._tracker.add_listener(sync_wrapper)
            self._listeners[listener_id] = sync_wrapper

    async def register_flag_value_change_listener(
        self,
        listener_id: str,
        flag_key: str,
        context: Context,
        callback_uri: str,
    ):
        import aiohttp

        loop = asyncio.get_running_loop()

        def on_value_change(change):
            payload = {
                'listenerId': listener_id,
                'flagKey': change.key,
                'oldValue': change.old_value,
                'newValue': change.new_value,
            }

            async def _post():
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.post(callback_uri, json=payload)
                except Exception as e:
                    global_log.warning('Failed to post flag value change notification: %s', e)

            asyncio.run_coroutine_threadsafe(_post(), loop)

        async with self._lock:
            if listener_id in self._listeners:
                old = self._listeners[listener_id]
                self._tracker.remove_listener(old)

            value_listener = await self._tracker.add_flag_value_change_listener(
                flag_key, context, on_value_change
            )
            self._listeners[listener_id] = value_listener

    async def unregister(self, listener_id: str) -> bool:
        async with self._lock:
            listener = self._listeners.pop(listener_id, None)
            if listener is None:
                return False
            self._tracker.remove_listener(listener)
            return True

    async def close_all(self):
        async with self._lock:
            for listener in self._listeners.values():
                self._tracker.remove_listener(listener)
            self._listeners.clear()
