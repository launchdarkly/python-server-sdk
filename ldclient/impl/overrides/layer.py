"""
The override store. It holds the entries an override source has loaded, keyed by flag key or
segment key, and is replaced wholesale on each update.
"""

from typing import Any, Dict, Mapping, Optional, Set, Tuple

from ldclient.impl.dependency_tracker import DependencyTracker, KindAndKey
from ldclient.impl.model import ModelEntity
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

LayerContents = Dict[VersionedDataKind, Dict[str, ModelEntity]]


def _empty_contents() -> LayerContents:
    return {FEATURES: {}, SEGMENTS: {}}


class DeletedItemError(ValueError):
    """
    Indicates that a snapshot holds a flag or segment marked as deleted. The layer never holds a
    deleted item, so it rejects the whole snapshot. An override is removed by leaving it out of
    the snapshot.
    """

    def __init__(self, kind_name: str, key: str):
        super().__init__('%s "%s" is marked as deleted; an override is removed by leaving it out of the snapshot' % (kind_name, key))
        self.kind_name = kind_name
        self.key = key


class OverrideLayer:
    """
    A thread-safe store of override entries, replaced wholesale on each update from an override
    source. Each entry is a marked shallow copy of the definition the source supplied, so the
    source's own objects are never marked and a source may retain and resupply them.
    """

    def __init__(self):
        self._lock = ReadWriteLock()
        self._contents: LayerContents = _empty_contents()
        # The dependency graph of the override entries, rebuilt with each replacement. The store
        # consults it so that a LaunchDarkly change to a prerequisite or a segment also notifies
        # the override entries that depend on it.
        self._dependencies = DependencyTracker()
        # Read without the lock on every evaluation. A single attribute read is atomic, so the
        # per-evaluation cost of a configured but unpopulated override layer is negligible.
        self._non_empty = False

    def set_all(self, flags: Mapping[str, Any], segments: Mapping[str, Any]) -> Tuple[LayerContents, LayerContents]:
        """
        Atomically replaces the entire layer contents. Empty mappings clear the layer.

        Values may be model objects or their JSON dictionary form. A dictionary is decoded with
        the model constructor, which raises ``ValueError`` for an invalid definition. A
        definition whose key differs from its key in the mapping raises ``ValueError`` too, and a
        flag or segment marked as deleted, in either form, raises :class:`DeletedItemError`,
        which is a ``ValueError``. Nothing is replaced in any of these cases.

        :return: the previous and the new contents. The returned dictionaries must not be modified.
        """
        decoded: Dict[VersionedDataKind, Dict[str, Any]] = {
            FEATURES: {key: FEATURES.decode(item) for key, item in flags.items()},
            SEGMENTS: {key: SEGMENTS.decode(item) for key, item in segments.items()},
        }
        for kind, items in decoded.items():
            for key, item in items.items():
                if item.deleted:
                    raise DeletedItemError("flag" if kind == FEATURES else "segment", key)
                # Evaluation reads an entry by its mapping key, while the all-flags report and
                # analytics events use the definition's key. A mismatch would serve one key's
                # override under another, so the whole snapshot is rejected.
                if item.key != key:
                    raise ValueError('the override %s entry "%s" has the key "%s"; the key of each definition must be its key in the snapshot'
                                     % ("flag" if kind == FEATURES else "segment", key, item.key))
        replacement: LayerContents = {kind: {key: item.with_override_marker() for key, item in items.items()} for kind, items in decoded.items()}
        count = len(replacement[FEATURES]) + len(replacement[SEGMENTS])
        dependencies = DependencyTracker()
        for kind, items in replacement.items():
            for key, item in items.items():
                try:
                    dependencies.update_dependencies_from(kind, key, item)
                except Exception:
                    # An entry whose dependency fields cannot be read contributes no edges. It is
                    # still served, and the remaining entries are still tracked.
                    pass
        with self._lock.write():
            previous = self._contents
            self._contents = replacement
            self._dependencies = dependencies
            self._non_empty = count > 0
        return previous, replacement

    def add_affected_items(self, items_out: Set[KindAndKey], initial_modified_item: KindAndKey) -> None:
        """
        Adds the given item and every override entry that directly or indirectly depends on it
        to the set. The item itself need not be an override entry: a LaunchDarkly flag or
        segment that an override entry references is a valid starting point.
        """
        with self._lock.read():
            dependencies = self._dependencies
        dependencies.add_affected_items(items_out, initial_modified_item)

    def get(self, kind: VersionedDataKind, key: str) -> Optional[ModelEntity]:
        """Returns the override entry for a key, or None when the layer has none."""
        if not self._non_empty:
            return None
        with self._lock.read():
            return self._contents.get(kind, {}).get(key)

    def all(self, kind: VersionedDataKind) -> Dict[str, ModelEntity]:
        """Returns the entries of the given kind. The returned dictionary must not be modified."""
        with self._lock.read():
            return self._contents.get(kind, {})

    @property
    def is_empty(self) -> bool:
        """True when the layer holds no entries."""
        return not self._non_empty
