"""
The sink that applies override snapshots to the layer and notifies flag change listeners of the
flags whose evaluation may have changed.
"""

import threading
from typing import Any, Callable, Dict, Optional, Set

from ldclient.impl.dependency_tracker import DependencyTracker, KindAndKey
from ldclient.impl.overrides.layer import LayerContents, OverrideLayer
from ldclient.impl.util import log
from ldclient.interfaces import OverrideSink, ReadOnlyStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

MergedView = Dict[VersionedDataKind, Dict[str, Any]]

_DIFF_KINDS = (FEATURES, SEGMENTS)


class OverrideSinkImpl(OverrideSink):
    """
    Applies override layer replacements supplied by an override source, then notifies flag
    change listeners of every flag whose merged-view evaluation may have changed. Calls are
    serialized, so overlapping updates from a source cannot interleave.
    """

    def __init__(
        self,
        layer: OverrideLayer,
        base: ReadOnlyStore,
        notify: Callable[[str], None],
        has_listeners: Callable[[], bool],
    ):
        """
        :param layer: the override layer to write to
        :param base: the store holding LaunchDarkly data, without the overlay. Merged-view
          snapshots for change computation are built from it plus the layer.
        :param notify: receives the key of each affected flag
        :param has_listeners: reports whether anything listens for flag changes, so the change
          computation can be skipped when nothing does
        """
        self._layer = layer
        self._base = base
        self._notify = notify
        self._has_listeners = has_listeners
        self._lock = threading.Lock()

    def set_overrides(self, flags: Dict[str, Any], segments: Dict[str, Any]) -> None:  # type: ignore[override]
        with self._lock:
            if not self._has_listeners():
                self._layer.set_all(flags, segments)
                return

            previous, current = self._layer.set_all(flags, segments)
            old_merged = snapshot_merged_view(self._base, previous)
            new_merged = snapshot_merged_view(self._base, current)
            affected = compute_affected_flags(previous, current, old_merged, new_merged)
            if len(affected) > 0:
                log.debug("Override update affected %d flag(s)", len(affected))
            for key in sorted(affected):
                self._notify(key)


def snapshot_merged_view(base: ReadOnlyStore, overrides: LayerContents) -> MergedView:
    """
    Captures the merged view of a base store and a layer snapshot: base data with the override
    entries overlaid. A base read failure for a kind yields just the overrides for that kind.
    This degrades the dependency fan-out but never loses the directly changed keys.
    """
    view: MergedView = {}
    for kind in _DIFF_KINDS:
        items: Dict[str, Any] = {}
        try:
            base_items = base.all(kind, lambda x: x)
            if base_items is not None:
                items.update(base_items)
        except Exception as e:
            log.debug("Unable to read %s for override change computation: %s", kind.namespace, e)
        items.update(overrides.get(kind, {}))
        view[kind] = items
    return view


def compute_affected_flags(
    old_overrides: LayerContents,
    new_overrides: LayerContents,
    old_merged: MergedView,
    new_merged: MergedView,
) -> Set[str]:
    """
    Returns the keys of all flags whose merged-view evaluation may have changed when the
    override layer was replaced. The result includes the flags whose override entries were
    added, removed, or changed. Dependency fan-out adds every flag that depends, directly or
    transitively, on any added, removed, or changed entry of either kind.
    """
    seeds = diff_overrides(old_overrides, new_overrides)
    if len(seeds) == 0:
        return set()

    # Dependency edges are computed over both the old and the new merged views, because a
    # replacement can rewire dependencies. For example, removing a flag override restores the
    # prerequisite edges of the LaunchDarkly definition. Flags that depended on the override's
    # references exist as dependents only in the old view.
    old_tracker = _tracker_from_view(old_merged)
    new_tracker = _tracker_from_view(new_merged)
    affected: Set[KindAndKey] = set()
    for seed in seeds:
        old_tracker.add_affected_items(affected, seed)
        new_tracker.add_affected_items(affected, seed)
    return {item.key for item in affected if item.kind == FEATURES}


def diff_overrides(old_overrides: LayerContents, new_overrides: LayerContents) -> Set[KindAndKey]:
    """
    Returns a key for each entry whose override differs between the two layer snapshots. An
    added or removed entry is always a change, even when its content matches the underlying
    LaunchDarkly data, because the override marker alone changes the served entry. Entries
    present in both snapshots are compared by version and definition. The layer is rebuilt
    wholesale on every update, so identity comparison would report every retained entry as
    changed.
    """
    seeds: Set[KindAndKey] = set()
    for kind in _DIFF_KINDS:
        old_items = old_overrides.get(kind, {})
        new_items = new_overrides.get(kind, {})
        for key, old_item in old_items.items():
            new_item = new_items.get(key)
            if new_item is None or not _items_equal(old_item, new_item):
                seeds.add(KindAndKey(kind=kind, key=key))
        for key in new_items:
            if key not in old_items:
                seeds.add(KindAndKey(kind=kind, key=key))
    return seeds


def _items_equal(a: Any, b: Any) -> bool:
    if a.version != b.version:
        return False
    return a.to_json_dict() == b.to_json_dict()


def _tracker_from_view(view: MergedView) -> DependencyTracker:
    tracker = DependencyTracker()
    for kind in _DIFF_KINDS:
        for key, item in view.get(kind, {}).items():
            tracker.update_dependencies_from(kind, key, item)
    return tracker
