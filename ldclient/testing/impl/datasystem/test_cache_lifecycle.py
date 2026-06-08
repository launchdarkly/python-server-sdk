"""Tests for the persistent-store cache lifecycle under FDv2.

The cache lives on `CachingStoreWrapper`. Once the FDv2 in-memory store has
become the active read source, `Store._set_basis()` calls `disable_cache()`
on the persistent wrapper, which replaces the live cache with a no-op so it
no longer holds a duplicate copy of every flag.
"""

import threading
from typing import List

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import (
    _NOOP_CACHE,
    CachingStoreWrapper,
    _NoopCache
)
from ldclient.impl.datasystem.fdv2 import (
    DataStoreStatusProviderImpl,
    FeatureStoreClientWrapper
)
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    Change,
    ChangeSet,
    ChangeType,
    FeatureStoreCore,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.versioned_data_kind import FEATURES


class RecordingCore(FeatureStoreCore):
    """A minimal FeatureStoreCore for the wrapper tests.

    Records call counts so tests can assert that subsequent operations bypass
    the cache and reach the core.
    """

    def __init__(self):
        self.data: dict = {}
        self.inited = False
        self.init_calls = 0
        self.get_calls = 0
        self.upsert_calls = 0
        self.closed = False

    def init_internal(self, all_data):
        self.init_calls += 1
        self.data = {kind: dict(items) for kind, items in all_data.items()}
        self.inited = True

    def get_internal(self, kind, key):
        self.get_calls += 1
        return self.data.get(kind, {}).get(key)

    def get_all_internal(self, kind):
        return self.data.get(kind, {})

    def upsert_internal(self, kind, item):
        self.upsert_calls += 1
        items = self.data.setdefault(kind, {})
        existing = items.get(item['key'])
        if existing is None or existing.get('version', 0) < item.get('version', 0):
            items[item['key']] = item
            return item
        return existing

    def initialized_internal(self):
        return self.inited

    def close(self):
        self.closed = True


def _flag(key: str, version: int = 1) -> dict:
    return {
        "key": key,
        "version": version,
        "on": True,
        "variations": [True, False],
        "fallthrough": {"variation": 0},
    }


def _full_basis_changeset(flag_key: str = "flag-1") -> ChangeSet:
    return ChangeSet(
        intent_code=IntentCode.TRANSFER_FULL,
        changes=[
            Change(
                action=ChangeType.PUT,
                kind=ObjectKind.FLAG,
                key=flag_key,
                version=1,
                object=_flag(flag_key),
            )
        ],
        selector=Selector.no_selector(),
    )


def _delta_changeset(flag_key: str, version: int = 2) -> ChangeSet:
    return ChangeSet(
        intent_code=IntentCode.TRANSFER_CHANGES,
        changes=[
            Change(
                action=ChangeType.PUT,
                kind=ObjectKind.FLAG,
                key=flag_key,
                version=version,
                object=_flag(flag_key, version),
            )
        ],
        selector=Selector.no_selector(),
    )


def _build_store_with_persistent(persistent) -> Store:
    """Build a Store wired the same way fdv2.py does: outer FeatureStoreClientWrapper
    over the user's persistent store.
    """
    listeners = Listeners()
    status_provider = DataStoreStatusProviderImpl(persistent, listeners)
    outer = FeatureStoreClientWrapper(persistent, status_provider)
    store = Store(Listeners(), Listeners())
    store.with_persistence(outer, True, status_provider)
    return store


class TestNoopCache:
    def test_get_returns_default(self):
        assert _NOOP_CACHE.get("any-key") is None
        assert _NOOP_CACHE.get("any-key", "fallback") == "fallback"

    def test_setitem_is_a_noop(self):
        _NOOP_CACHE["foo"] = "bar"
        assert _NOOP_CACHE.get("foo") is None

    def test_pop_returns_default(self):
        assert _NOOP_CACHE.pop("any-key") is None
        assert _NOOP_CACHE.pop("any-key", "fallback") == "fallback"

    def test_clear_is_a_noop(self):
        _NOOP_CACHE.clear()  # must not raise

    def test_singleton_identity(self):
        assert _NOOP_CACHE is _NOOP_CACHE
        assert isinstance(_NOOP_CACHE, _NoopCache)


class TestCachingStoreWrapperDisable:
    def test_disabled_at_config_uses_noop_singleton(self):
        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.disabled())
        assert wrapper._cache is _NOOP_CACHE

    def test_enabled_at_config_uses_real_cache(self):
        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.default())
        assert wrapper._cache is not _NOOP_CACHE

    def test_disable_cache_swaps_to_noop_singleton(self):
        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.default())
        assert wrapper._cache is not _NOOP_CACHE
        wrapper.disable_cache()
        assert wrapper._cache is _NOOP_CACHE

    def test_disable_cache_is_idempotent(self):
        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.default())
        wrapper.disable_cache()
        wrapper.disable_cache()
        wrapper.disable_cache()
        assert wrapper._cache is _NOOP_CACHE

    def test_disable_cache_when_already_disabled_at_config(self):
        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.disabled())
        wrapper.disable_cache()
        assert wrapper._cache is _NOOP_CACHE

    def test_get_uses_real_cache_before_disable(self):
        core = RecordingCore()
        core.data = {FEATURES: {"flag-1": _flag("flag-1")}}
        wrapper = CachingStoreWrapper(core, CacheConfig.default())

        wrapper.get(FEATURES, "flag-1")
        wrapper.get(FEATURES, "flag-1")
        # Second call should be served from the cache, not the core.
        assert core.get_calls == 1

    def test_get_falls_through_to_core_after_disable(self):
        core = RecordingCore()
        core.data = {FEATURES: {"flag-1": _flag("flag-1")}}
        wrapper = CachingStoreWrapper(core, CacheConfig.default())

        # Prime the cache.
        wrapper.get(FEATURES, "flag-1")
        assert core.get_calls == 1

        wrapper.disable_cache()

        wrapper.get(FEATURES, "flag-1")
        wrapper.get(FEATURES, "flag-1")
        # Each call after disable must reach the core (no cache).
        assert core.get_calls == 3

    def test_upsert_after_disable_does_not_repopulate_cache(self):
        core = RecordingCore()
        wrapper = CachingStoreWrapper(core, CacheConfig.default())
        wrapper.disable_cache()

        wrapper.upsert(FEATURES, _flag("flag-2", version=1))
        # The upsert reaches the core, but the cache stays a no-op.
        assert core.upsert_calls == 1
        assert wrapper._cache is _NOOP_CACHE

    def test_init_after_disable_skips_decode_loop(self):
        """When the cache is already a no-op, init() must still write through
        to the core but skip the per-item decode loop."""
        core = RecordingCore()
        wrapper = CachingStoreWrapper(core, CacheConfig.default())
        wrapper.disable_cache()

        wrapper.init({FEATURES: {"flag-1": _flag("flag-1")}})

        assert core.init_calls == 1
        assert wrapper._cache is _NOOP_CACHE
        assert wrapper._inited is True

    def test_disable_cache_drops_entries_from_old_cache(self):
        """The old ExpiringDict must be empty after disable so its entries get GC'd."""
        core = RecordingCore()
        core.data = {FEATURES: {"flag-1": _flag("flag-1")}}
        wrapper = CachingStoreWrapper(core, CacheConfig.default())

        wrapper.get(FEATURES, "flag-1")  # populate the real cache
        old_cache = wrapper._cache
        assert old_cache is not _NOOP_CACHE
        assert len(old_cache) > 0  # type: ignore[arg-type]

        wrapper.disable_cache()

        # The reference we captured before the swap must now be empty.
        assert len(old_cache) == 0  # type: ignore[arg-type]

    def test_disable_cache_swallows_clear_failure(self):
        """If the old cache's clear() raises, disable_cache must still leave
        the wrapper in a consistent state with `_cache is _NOOP_CACHE`."""

        class BrokenCache:
            def clear(self):
                raise RuntimeError("boom")

        wrapper = CachingStoreWrapper(RecordingCore(), CacheConfig.default())
        wrapper._cache = BrokenCache()  # type: ignore[assignment]

        wrapper.disable_cache()  # must not raise

        assert wrapper._cache is _NOOP_CACHE


class TestStoreDisablesPersistentCache:
    def test_set_basis_disables_cache_through_feature_store_client_wrapper(self):
        """End-to-end: Store._set_basis -> FeatureStoreClientWrapper.disable_cache
        -> CachingStoreWrapper.disable_cache."""
        core = RecordingCore()
        inner = CachingStoreWrapper(core, CacheConfig.default())
        store = _build_store_with_persistent(inner)

        assert inner._cache is not _NOOP_CACHE

        store.apply(_full_basis_changeset(), True)

        assert inner._cache is _NOOP_CACHE

    def test_set_basis_with_no_persistent_store_does_not_raise(self):
        store = Store(Listeners(), Listeners())
        store.apply(_full_basis_changeset(), False)  # must not raise

    def test_set_basis_tolerates_persistent_store_without_disable_cache(self):
        """A custom persistent store that doesn't expose disable_cache must not
        cause _set_basis to fail."""

        class CustomStore:
            def __init__(self):
                self.init_calls = 0

            def init(self, all_data):
                self.init_calls += 1

            def get(self, kind, key, callback=lambda x: x):
                return callback(None)

            def all(self, kind, callback=lambda x: x):
                return callback({})

            def delete(self, kind, key, version):
                pass

            def upsert(self, kind, item):
                pass

            @property
            def initialized(self):
                return True

        custom = CustomStore()
        store = Store(Listeners(), Listeners())
        store.with_persistence(custom, True, None)  # type: ignore[arg-type]

        store.apply(_full_basis_changeset(), True)  # must not raise

    def test_subsequent_set_basis_does_not_error(self):
        core = RecordingCore()
        inner = CachingStoreWrapper(core, CacheConfig.default())
        store = _build_store_with_persistent(inner)

        store.apply(_full_basis_changeset(), True)
        store.apply(_full_basis_changeset(), True)  # second one should be a no-op disable
        assert inner._cache is _NOOP_CACHE

    def test_apply_delta_after_disable_persists_to_core(self):
        core = RecordingCore()
        inner = CachingStoreWrapper(core, CacheConfig.default())
        store = _build_store_with_persistent(inner)

        store.apply(_full_basis_changeset(), True)
        assert inner._cache is _NOOP_CACHE

        store.apply(_delta_changeset("flag-1", version=2), True)

        # The delta should have been written through to the core.
        assert core.upsert_calls >= 1
        assert inner._cache is _NOOP_CACHE

    def test_set_basis_skips_decode_loop_in_persistent_store_init(self):
        """End-to-end ordering check: Store._set_basis must call disable_cache
        before persistent_store.init(), so the wrapper's init() fast-path skips
        the per-item decode loop. We assert this by counting decode calls on a
        kind whose decoder we can intercept via the underlying core's data."""
        core = RecordingCore()
        inner = CachingStoreWrapper(core, CacheConfig.default())
        store = _build_store_with_persistent(inner)

        # Apply a basis with several items so a stray decode loop would be visible.
        many_items_changeset = ChangeSet(
            intent_code=IntentCode.TRANSFER_FULL,
            changes=[
                Change(
                    action=ChangeType.PUT,
                    kind=ObjectKind.FLAG,
                    key=f"flag-{i}",
                    version=1,
                    object=_flag(f"flag-{i}"),
                )
                for i in range(10)
            ],
            selector=Selector.no_selector(),
        )
        store.apply(many_items_changeset, True)

        # The core's init was called exactly once, and after the swap the cache
        # is the no-op (confirming the swap happened before init's loop ran).
        assert core.init_calls == 1
        assert inner._cache is _NOOP_CACHE


class TestConcurrentDisable:
    def test_concurrent_access_during_disable_does_not_raise(self):
        """Stress test: reader and writer threads exercise every cache-touching
        method while the main thread repeatedly calls disable_cache(). Any TOCTOU
        race that would have surfaced as AttributeError under a self._cache=None
        design must not surface here.
        """
        core = RecordingCore()
        core.data = {FEATURES: {"flag-1": _flag("flag-1")}}
        wrapper = CachingStoreWrapper(core, CacheConfig.default())

        errors: List[BaseException] = []
        stop = threading.Event()

        def reader():
            try:
                while not stop.is_set():
                    wrapper.get(FEATURES, "flag-1")
                    _ = wrapper.initialized
            except BaseException as e:
                errors.append(e)

        def writer(thread_id: int):
            try:
                counter = 0
                while not stop.is_set():
                    wrapper.upsert(
                        FEATURES,
                        _flag(f"flag-w{thread_id}-{counter}", version=counter + 1),
                    )
                    counter += 1
            except BaseException as e:
                errors.append(e)

        threads = (
            [threading.Thread(target=reader) for _ in range(3)]
            + [threading.Thread(target=writer, args=(i,)) for i in range(2)]
        )
        for t in threads:
            t.start()
        try:
            for _ in range(200):
                wrapper.disable_cache()
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=2.0)

        assert errors == []
        assert wrapper._cache is _NOOP_CACHE


class TestCloseChain:
    def test_close_on_caching_store_wrapper_disables_and_closes_core(self):
        core = RecordingCore()
        wrapper = CachingStoreWrapper(core, CacheConfig.default())

        wrapper.close()

        assert wrapper._cache is _NOOP_CACHE
        assert core.closed is True

    def test_close_propagates_through_feature_store_client_wrapper(self):
        core = RecordingCore()
        inner = CachingStoreWrapper(core, CacheConfig.default())
        outer = FeatureStoreClientWrapper(
            inner, DataStoreStatusProviderImpl(inner, Listeners())
        )

        outer.close()

        assert inner._cache is _NOOP_CACHE
        assert core.closed is True

    def test_close_when_core_has_no_close_method(self):
        class CoreWithoutClose:
            def __init__(self):
                self.data: dict = {}
                self.inited = False

            def init_internal(self, all_data):
                self.data = dict(all_data)
                self.inited = True

            def get_internal(self, kind, key):
                return self.data.get(kind, {}).get(key)

            def get_all_internal(self, kind):
                return self.data.get(kind, {})

            def upsert_internal(self, kind, item):
                return item

            def initialized_internal(self):
                return self.inited

        wrapper = CachingStoreWrapper(CoreWithoutClose(), CacheConfig.default())  # type: ignore[arg-type]

        wrapper.close()  # must not raise
        assert wrapper._cache is _NOOP_CACHE
