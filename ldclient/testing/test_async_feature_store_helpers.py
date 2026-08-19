import asyncio

import pytest

from ldclient.async_feature_store_helpers import AsyncCachingStoreWrapper
from ldclient.feature_store import CacheConfig
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

# These tests exercise the caching-wrapper logic only, using an in-memory mock core, so they run
# without a Redis instance. They mirror ldclient.testing.test_feature_store_helpers for the sync
# CachingStoreWrapper.

THINGS = VersionedDataKind(namespace="things", request_api_path="", stream_api_path="")
WRONG_THINGS = VersionedDataKind(namespace="wrong", request_api_path="", stream_api_path="")


def make_wrapper(core, cached):
    return AsyncCachingStoreWrapper(core, CacheConfig(expiration=30) if cached else CacheConfig.disabled())


class MockAsyncCore:
    def __init__(self):
        self.data = {}
        self.inited = False
        self.inited_query_count = 0
        self.error = None

    async def init_internal(self, all_data):
        self._maybe_throw()
        self.data = {}
        for kind, items in all_data.items():
            self.data[kind] = items.copy()

    async def get_internal(self, kind, key):
        self._maybe_throw()
        items = self.data.get(kind)
        return None if items is None else items.get(key)

    async def get_all_internal(self, kind):
        self._maybe_throw()
        return self.data.get(kind)

    async def upsert_internal(self, kind, item):
        self._maybe_throw()
        key = item.get('key')
        items = self.data.get(kind)
        if items is None:
            items = {}
            self.data[kind] = items
        old_item = items.get(key)
        if old_item is None or old_item.get('version') < item.get('version'):
            items[key] = item
            return item
        return old_item

    async def initialized_internal(self):
        self._maybe_throw()
        self.inited_query_count = self.inited_query_count + 1
        return self.inited

    def _maybe_throw(self):
        if self.error is not None:
            raise self.error

    def force_set(self, kind, item):
        items = self.data.get(kind)
        if items is None:
            items = {}
            self.data[kind] = items
        items[item.get('key')] = item

    def force_remove(self, kind, key):
        items = self.data.get(kind)
        if items is not None:
            items.pop(key, None)


class AvailableCore(MockAsyncCore):
    def __init__(self, available):
        super().__init__()
        self._available = available

    async def is_available(self):
        return self._available


class CustomError(Exception):
    pass


class TestAsyncCachingStoreWrapper:
    @pytest.mark.parametrize("available", [False, True])
    def test_monitoring_enabled_if_available_is_defined(self, available: bool):
        wrapper = make_wrapper(AvailableCore(available), False)
        assert wrapper.is_monitoring_enabled() is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("available", [False, True])
    async def test_is_available_reflects_core(self, available: bool):
        wrapper = make_wrapper(AvailableCore(available), False)
        assert await wrapper.is_available() is available

    def test_monitoring_not_enabled_if_available_is_not_defined(self):
        wrapper = make_wrapper(MockAsyncCore(), False)
        assert wrapper.is_monitoring_enabled() is False

    @pytest.mark.asyncio
    async def test_is_available_false_if_not_defined(self):
        wrapper = make_wrapper(MockAsyncCore(), False)
        assert await wrapper.is_available() is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_item(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        core.force_set(THINGS, itemv1)
        assert await wrapper.get(THINGS, key) == itemv1

        core.force_set(THINGS, itemv2)
        # if cached, we will not see the new underlying value yet
        assert await wrapper.get(THINGS, key) == (itemv1 if cached else itemv2)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_deleted_item(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1, "deleted": True}
        itemv2 = {"key": key, "version": 2}

        core.force_set(THINGS, itemv1)
        assert await wrapper.get(THINGS, key) is None  # filtered out because deleted is true

        core.force_set(THINGS, itemv2)
        assert await wrapper.get(THINGS, key) == (None if cached else itemv2)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_missing_item(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        item = {"key": key, "version": 1}

        assert await wrapper.get(THINGS, key) is None

        core.force_set(THINGS, item)
        # the cache can retain a None result
        assert await wrapper.get(THINGS, key) == (None if cached else item)

    def test_cached_get_uses_values_from_init(self):
        async def run():
            core = MockAsyncCore()
            wrapper = make_wrapper(core, True)
            item1 = {"key": "flag1", "version": 1}
            item2 = {"key": "flag2", "version": 1}

            await wrapper.init({THINGS: {item1["key"]: item1, item2["key"]: item2}})
            core.force_remove(THINGS, item1["key"])
            assert await wrapper.get(THINGS, item1["key"]) == item1
        asyncio.run(run())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_can_throw_exception(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            await wrapper.get(THINGS, "key")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_all(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1}

        core.force_set(THINGS, item1)
        core.force_set(THINGS, item2)
        assert await wrapper.all(THINGS) == {item1["key"]: item1, item2["key"]: item2}

        core.force_remove(THINGS, item2["key"])
        if cached:
            assert await wrapper.all(THINGS) == {item1["key"]: item1, item2["key"]: item2}
        else:
            assert await wrapper.all(THINGS) == {item1["key"]: item1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_all_removes_deleted_items(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1, "deleted": True}

        core.force_set(THINGS, item1)
        core.force_set(THINGS, item2)
        assert await wrapper.all(THINGS) == {item1["key"]: item1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", [FEATURES, SEGMENTS])
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_all_tolerates_tombstone_with_no_key(self, cached, kind):
        # Other LaunchDarkly SDKs write deleted items to a persistent store with only the
        # version. The store knows the key, because it is the key the item is stored under.
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        live_item = {"key": "item1", "version": 1}
        tombstone = {"version": 2, "deleted": True}
        core.data[kind] = {"item1": live_item, "item2": tombstone}

        assert await wrapper.all(kind) == {"item1": kind.decode(live_item)}
        assert await wrapper.get(kind, "item2") is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_all_changes_None_to_empty_dict(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        assert await wrapper.all(WRONG_THINGS) == {}

    def test_cached_get_all_uses_values_from_init(self):
        async def run():
            core = MockAsyncCore()
            wrapper = make_wrapper(core, True)
            item1 = {"key": "flag1", "version": 1}
            item2 = {"key": "flag2", "version": 1}
            both = {item1["key"]: item1, item2["key"]: item2}

            await wrapper.init({THINGS: both})
            core.force_remove(THINGS, item1["key"])
            assert await wrapper.all(THINGS) == both
        asyncio.run(run())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_get_all_can_throw_exception(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            await wrapper.all(THINGS)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_upsert_successful(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        assert await wrapper.upsert(THINGS, itemv1) is True
        assert core.data[THINGS][key] == itemv1

        assert await wrapper.upsert(THINGS, itemv2) is True
        assert core.data[THINGS][key] == itemv2

        # if we have a cache, verify that the new item is now cached by writing a different value
        # to the underlying data - get should still return the cached item
        if cached:
            itemv3 = {"key": key, "version": 3}
            core.force_set(THINGS, itemv3)

        assert await wrapper.get(THINGS, key) == itemv2

    @pytest.mark.asyncio
    async def test_cached_upsert_unsuccessful(self):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, True)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        assert await wrapper.upsert(THINGS, itemv2) is True
        assert core.data[THINGS][key] == itemv2

        assert await wrapper.upsert(THINGS, itemv1) is False
        assert core.data[THINGS][key] == itemv2  # value in store remains the same

        itemv3 = {"key": key, "version": 3}
        core.force_set(THINGS, itemv3)  # bypasses cache so we can verify itemv2 is in the cache
        assert await wrapper.get(THINGS, key) == itemv2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_upsert_can_throw_exception(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            await wrapper.upsert(THINGS, {"key": "x", "version": 1})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_delete(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2, "deleted": True}
        itemv3 = {"key": key, "version": 3}

        core.force_set(THINGS, itemv1)
        assert await wrapper.get(THINGS, key) == itemv1

        assert await wrapper.delete(THINGS, key, 2) is True
        assert core.data[THINGS][key] == itemv2

        core.force_set(THINGS, itemv3)  # make a change that bypasses the cache
        assert await wrapper.get(THINGS, key) == (None if cached else itemv3)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("cached", [False, True])
    async def test_delete_can_throw_exception(self, cached):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            await wrapper.delete(THINGS, "x", 1)

    @pytest.mark.asyncio
    async def test_not_initialized_before_init(self):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, False)
        assert wrapper.initialized is False

    @pytest.mark.asyncio
    async def test_initialized_after_init(self):
        core = MockAsyncCore()
        wrapper = make_wrapper(core, False)
        await wrapper.init({})
        assert wrapper.initialized is True

    @pytest.mark.asyncio
    async def test_close_closes_core_if_supported(self):
        closed = {"value": False}

        class ClosableCore(MockAsyncCore):
            async def close(self):
                closed["value"] = True

        wrapper = make_wrapper(ClosableCore(), True)
        await wrapper.close()
        assert closed["value"] is True

    @pytest.mark.asyncio
    async def test_describe_configuration_delegates_to_core(self):
        class DescribedCore(MockAsyncCore):
            def describe_configuration(self, config):
                return "MyStore"

        wrapper = make_wrapper(DescribedCore(), False)
        assert wrapper.describe_configuration(None) == "MyStore"

    @pytest.mark.asyncio
    async def test_describe_configuration_defaults_to_custom(self):
        wrapper = make_wrapper(MockAsyncCore(), False)
        assert wrapper.describe_configuration(None) == "custom"
