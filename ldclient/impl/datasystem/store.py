"""
Store implementation for FDv2 data system.

This module provides a dual-mode persistent/in-memory store that serves requests for data
from the evaluation algorithm. It manages both in-memory and persistent storage, handling
ChangeSet applications and flag change notifications.
"""

import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

from ldclient.impl.datasystem.protocolv2 import (
    Change,
    ChangeSet,
    ChangeType,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.impl.dependency_tracker import DependencyTracker, KindAndKey
from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.util import log
from ldclient.interfaces import (
    DataStoreStatusProvider,
    FeatureStore,
    FlagChange,
    ReadOnlyStore
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

Collections = Dict[VersionedDataKind, Dict[str, dict]]


class InMemoryFeatureStore(ReadOnlyStore):
    """
    The default feature store implementation, which holds all data in a
    thread-safe data structure in memory.
    """

    def __init__(self):
        """Constructs an instance of InMemoryFeatureStore."""
        self._lock = ReadWriteLock()
        self._initialized = False
        self._items = defaultdict(dict)

    def get(
        self,
        kind: VersionedDataKind,
        key: str,
        callback: Callable[[Any], Any] = lambda x: x,
    ) -> Any:
        try:
            self._lock.rlock()
            items_of_kind = self._items[kind]
            item = items_of_kind.get(key)
            if item is None:
                log.debug(
                    "Attempted to get missing key %s in '%s', returning None",
                    key,
                    kind.namespace,
                )
                return callback(None)
            if "deleted" in item and item["deleted"]:
                log.debug(
                    "Attempted to get deleted key %s in '%s', returning None",
                    key,
                    kind.namespace,
                )
                return callback(None)
            return callback(item)
        finally:
            self._lock.runlock()

    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        try:
            self._lock.rlock()
            items_of_kind = self._items[kind]
            return callback(
                dict(
                    (k, i)
                    for k, i in items_of_kind.items()
                    if ("deleted" not in i) or not i["deleted"]
                )
            )
        finally:
            self._lock.runlock()

    def set_basis(self, collections: Collections):
        """
        Initializes the store with a full set of data, replacing any existing data.
        """
        try:
            self._lock.lock()

            all_decoded = {}
            for kind in collections:
                collection = collections[kind]
                items_decoded = {}
                for key in collection:
                    items_decoded[key] = kind.decode(collection[key])
                all_decoded[kind] = items_decoded

            self._items.clear()
            self._items.update(all_decoded)
            self._initialized = True
        finally:
            self._lock.unlock()

    def apply_delta(self, collections: Collections):
        """
        Applies a delta update to the store.
        """
        try:
            self._lock.lock()
            for kind in collections:
                collection = collections[kind]
                for key in collection:
                    self.__upsert(kind, collection[key])
        finally:
            self._lock.unlock()

    def __upsert(self, kind, item):
        """
        Inserts or updates an item in the store.
        """
        decoded_item = kind.decode(item)
        key = item["key"]
        items_of_kind = self._items[kind]
        items_of_kind[key] = decoded_item
        log.debug(
            "Updated %s in '%s' to version %d", key, kind.namespace, item["version"]
        )

    @property
    def initialized(self) -> bool:
        """
        Indicates whether the store has been initialized with data.
        """
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()


class Store:
    """
    Store is a dual-mode persistent/in-memory store that serves requests for
    data from the evaluation algorithm.

    At any given moment one of two stores is active: in-memory, or persistent.
    Once the in-memory store has data (either from initializers or a
    synchronizer), the persistent store is no longer read from. From that point
    forward, calls to get data will serve from the memory store.
    """

    def __init__(
        self,
        flag_change_listeners: Listeners,
        change_set_listeners: Listeners,
    ):
        """
        Initialize a new Store.

        Args:
            flag_change_listeners: Listeners for flag change events
            change_set_listeners: Listeners for changeset events
        """
        self._persistent_store: Optional[FeatureStore] = None
        self._persistent_store_status_provider: Optional[DataStoreStatusProvider] = None
        self._persistent_store_writable = False

        # Source of truth for flag evaluations once initialized
        self._memory_store = InMemoryFeatureStore()

        # Used to track dependencies between items in the store
        self._dependency_tracker = DependencyTracker()

        # Listeners for events
        self._flag_change_listeners = flag_change_listeners
        self._change_set_listeners = change_set_listeners

        # True if the data in the memory store may be persisted to the persistent store
        self._persist = False

        # Points to the active store. Swapped upon initialization.
        self._active_store: ReadOnlyStore = self._memory_store

        # Identifies the current data
        self._selector = Selector.no_selector()

        # Thread synchronization
        self._lock = threading.RLock()

    def with_persistence(
        self,
        persistent_store: FeatureStore,
        writable: bool,
        status_provider: Optional[DataStoreStatusProvider] = None,
    ) -> "Store":
        """
        Configure the store with a persistent store for read-only or read-write access.

        Args:
            persistent_store: The persistent store implementation
            writable: Whether the persistent store should be written to
            status_provider: Optional status provider for the persistent store

        Returns:
            Self for method chaining
        """
        with self._lock:
            self._persistent_store = persistent_store
            self._persistent_store_writable = writable
            self._persistent_store_status_provider = status_provider

            # Initially use persistent store as active until memory store has data
            self._active_store = persistent_store

        return self

    def selector(self) -> Selector:
        """Returns the current selector."""
        with self._lock:
            return self._selector

    def close(self) -> Optional[Exception]:
        """Close the store and any persistent store if configured."""
        with self._lock:
            if self._persistent_store is not None:
                try:
                    # Most FeatureStore implementations don't have close methods
                    # but we'll try to call it if it exists
                    if hasattr(self._persistent_store, "close"):
                        self._persistent_store.close()
                except Exception as e:
                    return e
        return None

    def apply(self, change_set: ChangeSet, persist: bool) -> None:
        """
        Apply a changeset to the store.

        Args:
            change_set: The changeset to apply
            persist: Whether the changes should be persisted to the persistent store
        """
        collections = self._changes_to_store_data(change_set.changes)

        with self._lock:
            try:
                if change_set.intent_code == IntentCode.TRANSFER_FULL:
                    self._set_basis(collections, change_set.selector, persist)
                elif change_set.intent_code == IntentCode.TRANSFER_CHANGES:
                    self._apply_delta(collections, change_set.selector, persist)
                elif change_set.intent_code == IntentCode.TRANSFER_NONE:
                    # No-op, no changes to apply
                    return

                # Notify changeset listeners
                self._change_set_listeners.notify(change_set)

            except Exception as e:
                # Log error but don't re-raise - matches Go behavior
                log.error("Store: couldn't apply changeset: %s", str(e))

    def _set_basis(
        self, collections: Collections, selector: Optional[Selector], persist: bool
    ) -> None:
        """
        Set the basis of the store. Any existing data is discarded.

        Args:
            change_set: The changeset containing the new basis data
            persist: Whether to persist the data to the persistent store
        """
        # Take snapshot for change detection if we have flag listeners
        old_data: Optional[Collections] = None
        if self._flag_change_listeners.has_listeners():
            old_data = {}
            for kind in [FEATURES, SEGMENTS]:
                old_data[kind] = self._memory_store.all(kind, lambda x: x)

        self._memory_store.set_basis(collections)

        # Update dependency tracker
        self._reset_dependency_tracker(collections)

        # Send change events if we had listeners
        if old_data is not None:
            affected_items = self._compute_changed_items_for_full_data_set(
                old_data, collections
            )
            self._send_change_events(affected_items)

        # Update state
        self._persist = persist
        self._selector = selector if selector is not None else Selector.no_selector()

        # Switch to memory store as active
        self._active_store = self._memory_store

        # Persist to persistent store if configured and writable
        if self._should_persist():
            self._persistent_store.init(collections)  # type: ignore

    def _apply_delta(
        self, collections: Collections, selector: Optional[Selector], persist: bool
    ) -> None:
        """
        Apply a delta update to the store.

        Args:
            change_set: The changeset containing the delta changes
            persist: Whether to persist the changes to the persistent store
        """
        self._memory_store.apply_delta(collections)

        has_listeners = self._flag_change_listeners.has_listeners()
        affected_items: Set[KindAndKey] = set()

        for kind in collections:
            collection = collections[kind]
            for key in collection:
                item = collection[key]
                self._dependency_tracker.update_dependencies_from(
                    kind, key, item
                )
                if has_listeners:
                    self._dependency_tracker.add_affected_items(
                        affected_items, KindAndKey(kind=kind, key=key)
                    )

        # Send change events
        if affected_items:
            self._send_change_events(affected_items)

        # Update state
        self._persist = persist
        self._selector = selector if selector is not None else Selector.no_selector()

        if self._should_persist():
            for kind in collections:
                kind_data: Dict[str, dict] = collections[kind]
                for i in kind_data:
                    item = kind_data[i]
                    self._persistent_store.upsert(kind, item)  # type: ignore

    def _should_persist(self) -> bool:
        """Returns whether data should be persisted to the persistent store."""
        return (
            self._persist
            and self._persistent_store is not None
            and self._persistent_store_writable
        )

    def _changes_to_store_data(self, changes: List[Change]) -> Collections:
        """
        Convert a list of Changes to the pre-existing format used by FeatureStore.

        Args:
            changes: List of changes to convert

        Returns:
            Mapping suitable for FeatureStore operations.
        """
        all_data: Collections = {
            FEATURES: {},
            SEGMENTS: {},
        }

        for change in changes:
            kind = FEATURES if change.kind == ObjectKind.FLAG else SEGMENTS
            if change.action == ChangeType.PUT and change.object is not None:
                all_data[kind][change.key] = change.object
            if change.action == ChangeType.DELETE:
                all_data[kind][change.key] = {'key': change.key, 'deleted': True, 'version': change.version}

        return all_data

    def _reset_dependency_tracker(self, all_data: Collections) -> None:
        """Reset dependency tracker with new full data set."""
        self._dependency_tracker.reset()
        for kind, items in all_data.items():
            for key, item in items.items():
                self._dependency_tracker.update_dependencies_from(kind, key, item)

    def _send_change_events(self, affected_items: Set[KindAndKey]) -> None:
        """Send flag change events for affected items."""
        for item in affected_items:
            if item.kind == FEATURES:
                self._flag_change_listeners.notify(FlagChange(item.key))

    def _compute_changed_items_for_full_data_set(
        self,
        old_data: Collections,
        new_data: Collections,
    ) -> Set[KindAndKey]:
        """Compute which items changed between old and new data sets."""
        affected_items: Set[KindAndKey] = set()

        for kind in [FEATURES, SEGMENTS]:
            old_items = old_data.get(kind, {})
            new_items = new_data.get(kind, {})

            # Get all keys from both old and new data
            all_keys = set(old_items.keys()) | set(new_items.keys())

            for key in all_keys:
                old_item = old_items.get(key)
                new_item = new_items.get(key)

                # If either is missing or versions differ, it's a change
                if old_item is None or new_item is None:
                    self._dependency_tracker.add_affected_items(
                        affected_items, KindAndKey(kind=kind, key=key)
                    )
                elif old_item.get("version", 0) != new_item.get("version", 0):
                    self._dependency_tracker.add_affected_items(
                        affected_items, KindAndKey(kind=kind, key=key)
                    )

        return affected_items

    def commit(self) -> Optional[Exception]:
        """
        Commit persists the data in the memory store to the persistent store, if configured.

        Returns:
            Exception if commit failed, None otherwise
        """
        with self._lock:
            if self._should_persist():
                try:
                    # Get all data from memory store and write to persistent store
                    all_data = {}
                    for kind in [FEATURES, SEGMENTS]:
                        all_data[kind] = self._memory_store.all(kind, lambda x: x)
                    self._persistent_store.init(all_data)  # type: ignore
                except Exception as e:
                    return e
        return None

    def get_active_store(self) -> ReadOnlyStore:
        """Get the currently active store for reading data."""
        with self._lock:
            return self._active_store

    def is_initialized(self) -> bool:
        """Check if the active store is initialized."""
        return self.get_active_store().initialized

    def get_data_store_status_provider(self) -> Optional[DataStoreStatusProvider]:
        """Get the data store status provider for the persistent store, if configured."""
        with self._lock:
            return self._persistent_store_status_provider
