"""
The override store. It holds the entries an override source has loaded, keyed by flag key or
segment key, and is replaced wholesale on each update.
"""

from typing import Any, Dict, Mapping, Optional, Tuple

from ldclient.impl.model import ModelEntity
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

LayerContents = Dict[VersionedDataKind, Dict[str, ModelEntity]]


def _empty_contents() -> LayerContents:
    return {FEATURES: {}, SEGMENTS: {}}


class OverrideLayer:
    """
    A thread-safe store of override entries, replaced wholesale on each update from an override
    source. Each entry is a marked shallow copy of the definition the source supplied, so the
    source's own objects are never marked and a source may retain and resupply them.
    """

    def __init__(self):
        self._lock = ReadWriteLock()
        self._contents: LayerContents = _empty_contents()
        # Read without the lock on every evaluation. A single attribute read is atomic, so the
        # per-evaluation cost of a configured but unpopulated override layer is negligible.
        self._non_empty = False

    def set_all(self, flags: Mapping[str, Any], segments: Mapping[str, Any]) -> Tuple[LayerContents, LayerContents]:
        """
        Atomically replaces the entire layer contents. Empty mappings clear the layer.

        Values may be model objects or their JSON dictionary form. A dictionary is decoded with
        the model constructor, which raises ``ValueError`` for an invalid definition, and nothing
        is replaced in that case.

        :return: the previous and the new contents. The returned dictionaries must not be modified.
        """
        replacement: LayerContents = {
            FEATURES: {key: FEATURES.decode(item).with_override_marker() for key, item in flags.items()},
            SEGMENTS: {key: SEGMENTS.decode(item).with_override_marker() for key, item in segments.items()},
        }
        count = len(replacement[FEATURES]) + len(replacement[SEGMENTS])
        with self._lock.write():
            previous = self._contents
            self._contents = replacement
            self._non_empty = count > 0
        return previous, replacement

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
