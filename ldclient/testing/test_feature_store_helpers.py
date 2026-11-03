import json
from time import sleep

import pytest

from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.interfaces import FeatureStoreCore, SerializedItemDescriptor
from ldclient.testing.serialized_items import (
    from_descriptor,
    from_descriptors,
    to_descriptor,
    to_descriptors
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

THINGS = VersionedDataKind(namespace="things", request_api_path="", stream_api_path="")
WRONG_THINGS = VersionedDataKind(namespace="wrong", request_api_path="", stream_api_path="")


def make_wrapper(core, cached):
    return CachingStoreWrapper(core, CacheConfig(expiration=30) if cached else CacheConfig.disabled())


class MockCore(FeatureStoreCore):
    def __init__(self):
        self.data = {}
        self.inited = False
        self.inited_query_count = 0
        self.error = None
        self.available = True
        self.closed = False

    def is_available(self):
        return self.available

    def close(self):
        self.closed = True

    def init_internal(self, all_data):
        self._maybe_throw()
        self.data = from_descriptors(all_data)

    def get_internal(self, kind, key):
        self._maybe_throw()
        items = self.data.get(kind)
        return None if items is None else to_descriptor(items.get(key))

    def get_all_internal(self, kind):
        self._maybe_throw()
        return to_descriptors(self.data.get(kind))

    def upsert_internal(self, kind, key, descriptor):
        self._maybe_throw()
        item = from_descriptor(descriptor)
        items = self.data.get(kind)
        if items is None:
            items = {}
            self.data[kind] = items
        old_item = items.get(key)
        if old_item is None or old_item.get('version') < item.get('version'):
            items[key] = item
            return True
        return False

    def initialized_internal(self):
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


class CustomError(Exception):
    pass


class TestCachingStoreWrapper:
    def test_monitoring_is_enabled(self):
        wrapper = make_wrapper(MockCore(), False)
        assert wrapper.is_monitoring_enabled()

    @pytest.mark.parametrize("available", [False, True])
    def test_is_available_delegates_to_core(self, available: bool):
        core = MockCore()
        core.available = available
        wrapper = make_wrapper(core, False)
        assert wrapper.is_available() == available

    def test_close_closes_core(self):
        core = MockCore()
        wrapper = make_wrapper(core, False)
        wrapper.close()
        assert core.closed

    @pytest.mark.parametrize("missing", ["is_available", "close"])
    def test_core_without_required_method_cannot_be_instantiated(self, missing: str):
        methods = {name: getattr(MockCore, name) for name in ("is_available", "close")}
        del methods[missing]
        core_class = type("IncompleteCore", (FeatureStoreCore,), {
            "get_internal": MockCore.get_internal,
            "get_all_internal": MockCore.get_all_internal,
            "init_internal": MockCore.init_internal,
            "upsert_internal": MockCore.upsert_internal,
            "initialized_internal": MockCore.initialized_internal,
            **methods,
        })
        with pytest.raises(TypeError):
            core_class()

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_item(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        core.force_set(THINGS, itemv1)
        assert wrapper.get(THINGS, key) == itemv1

        core.force_set(THINGS, itemv2)
        assert wrapper.get(THINGS, key) == (itemv1 if cached else itemv2)  # if cached, we will not see the new underlying value yet

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_deleted_item(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1, "deleted": True}
        itemv2 = {"key": key, "version": 2}

        core.force_set(THINGS, itemv1)
        assert wrapper.get(THINGS, key) is None  # item is filtered out because deleted is true

        core.force_set(THINGS, itemv2)
        assert wrapper.get(THINGS, key) == (None if cached else itemv2)  # if cached, we will not see the new underlying value yet

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_missing_item(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        item = {"key": key, "version": 1}

        assert wrapper.get(THINGS, key) is None

        core.force_set(THINGS, item)
        assert wrapper.get(THINGS, key) == (None if cached else item)  # the cache can retain a nil result

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_with_lambda(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        item = {"key": key, "version": 1}
        modified_item = {"key": key, "version": 99}

        core.force_set(THINGS, item)
        assert wrapper.get(THINGS, key, lambda x: modified_item) == modified_item

    def test_cached_get_uses_values_from_init(self):
        core = MockCore()
        wrapper = make_wrapper(core, True)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1}

        wrapper.init({THINGS: {item1["key"]: item1, item2["key"]: item2}})
        core.force_remove(THINGS, item1["key"])
        assert wrapper.get(THINGS, item1["key"]) == item1

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_can_throw_exception(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            wrapper.get(THINGS, "key", lambda x: x)

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1}

        core.force_set(THINGS, item1)
        core.force_set(THINGS, item2)
        assert wrapper.all(THINGS) == {item1["key"]: item1, item2["key"]: item2}

        core.force_remove(THINGS, item2["key"])
        if cached:
            assert wrapper.all(THINGS) == {item1["key"]: item1, item2["key"]: item2}
        else:
            assert wrapper.all(THINGS) == {item1["key"]: item1}

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all_removes_deleted_items(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1, "deleted": True}

        core.force_set(THINGS, item1)
        core.force_set(THINGS, item2)
        assert wrapper.all(THINGS) == {item1["key"]: item1}

    @pytest.mark.parametrize("kind", [FEATURES, SEGMENTS])
    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all_tolerates_tombstone_with_no_key(self, cached, kind):
        # Other LaunchDarkly SDKs write deleted items to a persistent store with only the
        # version. The store knows the key, because it is the key the item is stored under.
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        live_item = {"key": "item1", "version": 1}
        tombstone = {"version": 2, "deleted": True}
        core.data[kind] = {"item1": live_item, "item2": tombstone}

        assert wrapper.all(kind) == {"item1": kind.decode(live_item)}
        assert wrapper.get(kind, "item2") is None

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all_changes_None_to_empty_dict(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)

        assert wrapper.all(WRONG_THINGS) == {}

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all_iwith_lambda(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        extra = {"extra": True}
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1}
        core.force_set(THINGS, item1)
        core.force_set(THINGS, item2)
        assert wrapper.all(THINGS, lambda x: dict(x, **extra)) == {item1["key"]: item1, item2["key"]: item2, "extra": True}

    def test_cached_get_all_uses_values_from_init(self):
        core = MockCore()
        wrapper = make_wrapper(core, True)
        item1 = {"key": "flag1", "version": 1}
        item2 = {"key": "flag2", "version": 1}
        both = {item1["key"]: item1, item2["key"]: item2}

        wrapper.init({THINGS: both})
        core.force_remove(THINGS, item1["key"])
        assert wrapper.all(THINGS) == both

    @pytest.mark.parametrize("cached", [False, True])
    def test_get_all_can_throw_exception(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            wrapper.all(THINGS)

    @pytest.mark.parametrize("cached", [False, True])
    def test_upsert_successful(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        assert wrapper.upsert(THINGS, itemv1) is True
        assert core.data[THINGS][key] == itemv1

        assert wrapper.upsert(THINGS, itemv2) is True
        assert core.data[THINGS][key] == itemv2

        # if we have a cache, verify that the new item is now cached by writing a different value
        # to the underlying data - Get should still return the cached item
        if cached:
            itemv3 = {"key": key, "version": 3}
            core.force_set(THINGS, itemv3)

        assert wrapper.get(THINGS, key) == itemv2

    def test_cached_upsert_unsuccessful(self):
        # This is for an upsert where the data in the store has a higher version. In an uncached
        # store, this is just a no-op as far as the wrapper is concerned so there's nothing to
        # test here. In a cached store, we need to verify that the cache has been refreshed
        # using the data that was found in the store.
        core = MockCore()
        wrapper = make_wrapper(core, True)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2}

        assert wrapper.upsert(THINGS, itemv2) is True
        assert core.data[THINGS][key] == itemv2

        assert wrapper.upsert(THINGS, itemv1) is False
        assert core.data[THINGS][key] == itemv2  # value in store remains the same

        itemv3 = {"key": key, "version": 3}
        core.force_set(THINGS, itemv3)  # bypasses cache so we can verify that itemv2 is in the cache
        assert wrapper.get(THINGS, key) == itemv2

    @pytest.mark.parametrize("cached", [False, True])
    def test_upsert_can_throw_exception(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            wrapper.upsert(THINGS, {"key": "x", "version": 1})

    @pytest.mark.parametrize("cached", [False, True])
    def test_delete(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        key = "flag"
        itemv1 = {"key": key, "version": 1}
        itemv2 = {"key": key, "version": 2, "deleted": True}
        itemv3 = {"key": key, "version": 3}

        core.force_set(THINGS, itemv1)
        assert wrapper.get(THINGS, key) == itemv1

        assert wrapper.delete(THINGS, key, 2) is True
        assert core.data[THINGS][key] == itemv2

        core.force_set(THINGS, itemv3)  # make a change that bypasses the cache
        assert wrapper.get(THINGS, key) == (None if cached else itemv3)

    @pytest.mark.parametrize("cached", [False, True])
    def test_delete_can_throw_exception(self, cached):
        core = MockCore()
        wrapper = make_wrapper(core, cached)
        core.error = CustomError()
        with pytest.raises(CustomError):
            wrapper.delete(THINGS, "x", 1)

    def test_uncached_initialized_queries_state_only_until_inited(self):
        core = MockCore()
        wrapper = make_wrapper(core, False)

        assert wrapper.initialized is False
        assert core.inited_query_count == 1

        core.inited = True
        assert wrapper.initialized is True
        assert core.inited_query_count == 2

        core.inited = False
        assert wrapper.initialized is True
        assert core.inited_query_count == 2

    def test_uncached_initialized_does_not_query_state_if_init_was_called(self):
        core = MockCore()
        wrapper = make_wrapper(core, False)

        assert wrapper.initialized is False
        assert core.inited_query_count == 1

        wrapper.init({})

        assert wrapper.initialized is True
        assert core.inited_query_count == 1

    def test_cached_initialized_can_cache_false_result(self):
        core = MockCore()
        wrapper = CachingStoreWrapper(core, CacheConfig(expiration=0.2))  # use a shorter cache TTL for this test

        assert wrapper.initialized is False
        assert core.inited_query_count == 1

        core.inited = True
        assert wrapper.initialized is False
        assert core.inited_query_count == 1

        sleep(0.5)

        assert wrapper.initialized is True
        assert core.inited_query_count == 2

        # From this point on it should remain true and the method should not be called
        assert wrapper.initialized is True
        assert core.inited_query_count == 2


class DescriptorCore(MockCore):
    """Returns fixed serialized items and records the serialized items that it receives."""

    def __init__(self, returned=None):
        super().__init__()
        self.returned = returned
        self.received = []

    def get_internal(self, kind, key):
        return self.returned

    def get_all_internal(self, kind):
        return {'item': self.returned}

    def upsert_internal(self, kind, key, descriptor):
        self.received.append((key, descriptor))
        return True


@pytest.mark.parametrize("cached", [False, True])
def test_a_version_from_the_core_replaces_the_serialized_version(cached):
    core = DescriptorCore(SerializedItemDescriptor(version=5, deleted=False, serialized_item=json.dumps({"key": "item", "version": 3})))
    wrapper = make_wrapper(core, cached)

    assert wrapper.get(THINGS, "item") == {"key": "item", "version": 5}


@pytest.mark.parametrize("cached", [False, True])
def test_a_version_of_zero_from_the_core_keeps_the_serialized_version(cached):
    core = DescriptorCore(SerializedItemDescriptor(version=0, deleted=False, serialized_item=json.dumps({"key": "item", "version": 3})))
    wrapper = make_wrapper(core, cached)

    assert wrapper.get(THINGS, "item") == {"key": "item", "version": 3}


@pytest.mark.parametrize("cached", [False, True])
def test_an_item_that_the_core_marks_deleted_is_not_returned(cached):
    core = DescriptorCore(SerializedItemDescriptor(version=4, deleted=True, serialized_item="placeholder"))
    wrapper = make_wrapper(core, cached)

    assert wrapper.get(THINGS, "item") is None
    assert wrapper.all(THINGS) == {}


@pytest.mark.parametrize("cached", [False, True])
def test_upsert_gives_the_core_the_key_version_and_serialized_item(cached):
    core = DescriptorCore()
    wrapper = make_wrapper(core, cached)
    item = {"key": "item", "version": 2}

    assert wrapper.upsert(THINGS, item) is True

    assert core.received == [("item", SerializedItemDescriptor(version=2, deleted=False, serialized_item=json.dumps(item)))]


@pytest.mark.parametrize("cached", [False, True])
def test_delete_gives_the_core_a_serialized_placeholder(cached):
    core = DescriptorCore()
    wrapper = make_wrapper(core, cached)

    assert wrapper.delete(THINGS, "item", 7) is True

    [(key, descriptor)] = core.received
    assert key == "item"
    assert descriptor.version == 7
    assert descriptor.deleted is True
    assert json.loads(descriptor.serialized_item) == {"key": "item", "version": 7, "deleted": True}
