"""
The overlay at the store read boundary. A read for a key returns the override entry when one
exists and the LaunchDarkly entry otherwise. Targeting rules, prerequisites, and segment matches
behave identically for overridden and ordinary data, because they are the same reads through
the same boundary.
"""

from typing import Any, Callable, Dict, Optional

from ldclient.impl.overrides.layer import OverrideLayer
from ldclient.interfaces import AsyncReadOnlyStore, ReadOnlyStore
from ldclient.versioned_data_kind import VersionedDataKind


def _merge_all(base_items: Dict[str, Any], override_items: Dict[str, Any]) -> Dict[str, Any]:
    """The union of the base items and the override items. The override entry wins for a key present in both."""
    if len(override_items) == 0:
        return base_items
    merged = dict(base_items)
    merged.update(override_items)
    return merged


class OverrideStoreView(ReadOnlyStore):
    """
    Merges an override layer over a base read-only store.

    A per-key read serves an override entry whatever the state of the base store, so an
    uninitialized base still serves overrides. An enumeration is the union of the base items
    and the layer's items, and the override entry wins for a key present in both, including a
    key the base holds as a deleted item. A failure of the base enumeration propagates, as it
    does without the overlay: the layer's entries alone would look like a complete result.
    """

    def __init__(self, base: ReadOnlyStore, layer: OverrideLayer):
        self._base = base
        self._layer = layer

    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        item = self._layer.get(kind, key)
        if item is not None:
            return callback(item)
        return self._base.get(kind, key, callback)

    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        base_items = self._base.all(kind, lambda x: x)
        if base_items is None:
            # A store returns no result when it cannot read. That stays a failure rather than
            # becoming the layer's entries alone.
            return callback(None)
        return callback(_merge_all(base_items, self._layer.all(kind)))

    @property
    def initialized(self) -> bool:
        # The override layer never affects initialization status or data availability.
        return self._base.initialized


class AsyncOverrideStoreView(AsyncReadOnlyStore):
    """The async counterpart of :class:`OverrideStoreView`, over an async base store."""

    def __init__(self, base: AsyncReadOnlyStore, layer: OverrideLayer):
        self._base = base
        self._layer = layer

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        item = self._layer.get(kind, key)
        if item is not None:
            return item
        return await self._base.get(kind, key)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        return _merge_all(await self._base.all(kind), self._layer.all(kind))
