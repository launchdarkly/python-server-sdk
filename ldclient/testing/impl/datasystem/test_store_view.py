# pylint: disable=missing-docstring

from typing import Any, Callable, Dict

from ldclient.impl.datasystem.fdv1 import _ReadOnlyFeatureStoreView
from ldclient.impl.datasystem.fdv2 import _ReadOnlyStoreView
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    Change,
    ChangeSet,
    ChangeType,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.versioned_data_kind import FEATURES, VersionedDataKind


def _flag_dict(key: str, version: int) -> dict:
    return {
        "key": key,
        "version": version,
        "on": True,
        "variations": [True, False],
        "fallthrough": {"variation": 0},
    }


def _full_changeset(key: str, version: int) -> ChangeSet:
    return ChangeSet(
        intent_code=IntentCode.TRANSFER_FULL,
        changes=[Change(action=ChangeType.PUT, kind=ObjectKind.FLAG, key=key, version=version, object=_flag_dict(key, version))],
        selector=Selector.no_selector(),
    )


class FakeSyncFeatureStore:
    """Sync store: get/all take an optional callback and return the stored items."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        return callback(self._data.get(key))

    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        return callback(dict(self._data))

    @property
    def initialized(self) -> bool:
        return True


def test_feature_store_view_get_decodes_dict_and_passes_callback():
    raw = _flag_dict("flag-a", 1)
    view = _ReadOnlyFeatureStoreView(FakeSyncFeatureStore({"flag-a": raw}))  # type: ignore[arg-type]

    result = view.get(FEATURES, "flag-a")
    assert result == FEATURES.decode(raw)
    assert not isinstance(result, dict)

    key = view.get(FEATURES, "flag-a", lambda flag: flag.key if flag else None)
    assert key == "flag-a"


def test_feature_store_view_get_passes_through_model():
    decoded = FEATURES.decode(_flag_dict("flag-a", 1))
    view = _ReadOnlyFeatureStoreView(FakeSyncFeatureStore({"flag-a": decoded}))  # type: ignore[arg-type]
    assert view.get(FEATURES, "flag-a") is decoded


def test_feature_store_view_get_missing_returns_none():
    view = _ReadOnlyFeatureStoreView(FakeSyncFeatureStore({}))  # type: ignore[arg-type]
    assert view.get(FEATURES, "missing") is None


def test_feature_store_view_all_decodes_and_passes_callback():
    raw = _flag_dict("flag-a", 1)
    view = _ReadOnlyFeatureStoreView(FakeSyncFeatureStore({"flag-a": raw}))  # type: ignore[arg-type]

    result = view.all(FEATURES)
    assert result["flag-a"] == FEATURES.decode(raw)

    count = view.all(FEATURES, lambda items: len(items))
    assert count == 1


def test_feature_store_view_initialized_delegates():
    view = _ReadOnlyFeatureStoreView(FakeSyncFeatureStore({}))  # type: ignore[arg-type]
    assert view.initialized is True


def test_store_view_get_decodes_and_passes_callback():
    store = Store(Listeners(), Listeners())
    view = _ReadOnlyStoreView(store)

    store.apply(_full_changeset("flag-a", 1), False)

    result = view.get(FEATURES, "flag-a")
    assert result == FEATURES.decode(_flag_dict("flag-a", 1))
    assert not isinstance(result, dict)

    key = view.get(FEATURES, "flag-a", lambda flag: flag.key if flag else None)
    assert key == "flag-a"


def test_store_view_all_decodes_and_passes_callback():
    store = Store(Listeners(), Listeners())
    view = _ReadOnlyStoreView(store)
    store.apply(_full_changeset("flag-a", 1), False)

    result = view.all(FEATURES)
    assert set(result.keys()) == {"flag-a"}
    assert result["flag-a"] == FEATURES.decode(_flag_dict("flag-a", 1))

    count = view.all(FEATURES, lambda items: len(items))
    assert count == 1


def test_store_view_get_missing_returns_none():
    store = Store(Listeners(), Listeners())
    view = _ReadOnlyStoreView(store)
    store.apply(_full_changeset("flag-a", 1), False)
    assert view.get(FEATURES, "missing") is None


def test_store_view_initialized_delegates():
    store = Store(Listeners(), Listeners())
    view = _ReadOnlyStoreView(store)
    assert view.initialized is False
    store.apply(_full_changeset("flag-a", 1), False)
    assert view.initialized is True


def test_held_store_view_follows_active_store_swap():
    # Persistent (sync) store is active before memory has data.
    persistent = FakeSyncFeatureStore({"old-flag": _flag_dict("old-flag", 1)})
    store = Store(Listeners(), Listeners())
    store.with_persistence(persistent, False, None)  # type: ignore[arg-type]

    view = _ReadOnlyStoreView(store)  # held across the swap

    # Before init: reads hit the persistent store, decoded.
    before = view.get(FEATURES, "old-flag")
    assert before == FEATURES.decode(_flag_dict("old-flag", 1))

    # A full transfer swaps the active store to the in-memory store.
    store.apply(_full_changeset("new-flag", 1), False)

    # Same held view now follows the swap to the in-memory store.
    after = view.get(FEATURES, "new-flag")
    assert after == FEATURES.decode(_flag_dict("new-flag", 1))
    assert view.get(FEATURES, "old-flag") is None
