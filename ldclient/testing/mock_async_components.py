"""
Test utilities for async SDK components.
"""

from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.interfaces import EventProcessor, UpdateProcessor


class MockAsyncEventProcessor(EventProcessor):
    """A mock EventProcessor that records send_event() calls for testing.

    flush() and stop() are no-ops.
    """

    def __init__(self):
        self.events = []

    def send_event(self, event):
        self.events.append(event)

    def flush(self):
        pass

    def stop(self):
        pass


class MockAsyncFeatureStore(AsyncInMemoryFeatureStore):
    """A test wrapper around AsyncInMemoryFeatureStore that adds helper methods for test setup
    and records init() calls.
    """

    def __init__(self):
        super().__init__()
        self.inits = []

    async def init(self, all_data):
        self.inits.append(all_data)
        await super().init(all_data)

    async def force_set(self, kind, item):
        """Directly insert an item into the store, bypassing version checks.

        Useful for setting up test state without going through normal upsert semantics.
        """
        self._items[kind][item['key']] = item

    async def force_delete(self, kind, key):
        """Directly remove an item from the store.

        Useful for tearing down test state without going through the delete tombstone mechanism.
        """
        self._items[kind].pop(key, None)


class MockAsyncUpdateProcessor(UpdateProcessor):
    """A mock UpdateProcessor that immediately reports itself as initialized.

    Used to fake a ready data source in client tests. If a ``ready`` event is provided, it is
    set immediately in the constructor, matching the behavior of the sync ``MockUpdateProcessor``.
    """

    def __init__(self, config=None, store=None, ready=None):
        if ready is not None:
            ready.set()

    def start(self):
        pass

    def stop(self):
        pass

    def initialized(self) -> bool:
        return True

    def is_alive(self) -> bool:
        return True
