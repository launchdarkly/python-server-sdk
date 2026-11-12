"""
Tests of the public store conformance suites, against in-memory reference stores.

The reference stores share their data between instances with the same prefix, as a database
does. Each suite runs against a store that keeps the version and the deleted state separately,
and against one that keeps only the serialized item.
"""

import json
from typing import Callable, Dict, List, Optional, Tuple

import pytest

from ldclient.interfaces import (
    AsyncBigSegmentStore,
    AsyncFeatureStoreCore,
    BigSegmentStore,
    BigSegmentStoreMetadata,
    FeatureStoreCore,
    SerializedItemDescriptor
)
from ldclient.testhelpers.storetest import (
    AsyncBigSegmentStoreTestSuite,
    AsyncFeatureStoreCoreTestSuite,
    BigSegmentStoreTestSuite,
    FeatureStoreCoreTestSuite
)

_DATABASE: Dict[str, Dict[str, Dict[str, SerializedItemDescriptor]]] = {}
_INITED: Dict[str, bool] = {}
_BIG_SEGMENT_DATABASE: Dict[str, Tuple[Optional[int], Dict[str, Dict[str, bool]]]] = {}


def _prefix(prefix: Optional[str]) -> str:
    return prefix or 'default'


def _clear(prefix: Optional[str]):
    _DATABASE.pop(_prefix(prefix), None)
    _INITED.pop(_prefix(prefix), None)
    _BIG_SEGMENT_DATABASE.pop(_prefix(prefix), None)


class _ReferenceCore(FeatureStoreCore):
    def __init__(self, prefix: Optional[str], metadata_in_item_only: bool):
        self._prefix = _prefix(prefix)
        self._metadata_in_item_only = metadata_in_item_only
        self.hook: Optional[Callable[[], None]] = None
        self.broken = False

    def _items(self, kind) -> Dict[str, SerializedItemDescriptor]:
        return _DATABASE.setdefault(self._prefix, {}).setdefault(kind.namespace, {})

    def _check(self):
        if self.broken:
            raise RuntimeError('database is down')

    def _out(self, item: SerializedItemDescriptor) -> SerializedItemDescriptor:
        if self._metadata_in_item_only:
            return SerializedItemDescriptor(version=0, deleted=False, serialized_item=item.serialized_item)
        return item

    def get_internal(self, kind, key):
        self._check()
        item = self._items(kind).get(key)
        return None if item is None else self._out(item)

    def get_all_internal(self, kind):
        self._check()
        return {k: self._out(v) for k, v in self._items(kind).items()}

    def init_internal(self, all_data):
        self._check()
        _DATABASE[self._prefix] = {kind.namespace: dict(items) for kind, items in all_data.items()}
        _INITED[self._prefix] = True

    def upsert_internal(self, kind, key, item):
        self._check()
        if self.hook is not None:
            self.hook()
        old = self._items(kind).get(key)
        if old is not None and json.loads(old.serialized_item)['version'] >= item.version:
            return False
        self._items(kind)[key] = item
        return True

    def initialized_internal(self):
        self._check()
        return _INITED.get(self._prefix, False)

    def is_available(self):
        return not self.broken

    def close(self):
        pass


class _AsyncReferenceCore(AsyncFeatureStoreCore):
    def __init__(self, prefix: Optional[str], metadata_in_item_only: bool):
        self._core = _ReferenceCore(prefix, metadata_in_item_only)
        self.hook = None
        self.broken = False

    async def get_internal(self, kind, key):
        self._core.broken = self.broken
        return self._core.get_internal(kind, key)

    async def get_all_internal(self, kind):
        self._core.broken = self.broken
        return self._core.get_all_internal(kind)

    async def init_internal(self, all_data):
        self._core.broken = self.broken
        self._core.init_internal(all_data)

    async def upsert_internal(self, kind, key, item):
        self._core.broken = self.broken
        if self.broken:
            raise RuntimeError('database is down')
        if self.hook is not None:
            await self.hook()
        return self._core.upsert_internal(kind, key, item)

    async def initialized_internal(self):
        self._core.broken = self.broken
        return self._core.initialized_internal()

    async def is_available(self):
        return not self.broken

    async def close(self):
        pass


class _ReferenceBigSegmentStore(BigSegmentStore):
    def __init__(self, prefix: Optional[str]):
        self._prefix = _prefix(prefix)

    def get_metadata(self):
        return BigSegmentStoreMetadata(_BIG_SEGMENT_DATABASE.get(self._prefix, (None, {}))[0])

    def get_membership(self, context_hash):
        return _BIG_SEGMENT_DATABASE.get(self._prefix, (None, {}))[1].get(context_hash)

    def stop(self):
        pass


class _AsyncReferenceBigSegmentStore(AsyncBigSegmentStore):
    def __init__(self, prefix: Optional[str]):
        self._store = _ReferenceBigSegmentStore(prefix)

    async def get_metadata(self):
        return self._store.get_metadata()

    async def get_membership(self, context_hash):
        return self._store.get_membership(context_hash)

    async def stop(self):
        pass


def _set_metadata(prefix, metadata):
    memberships = _BIG_SEGMENT_DATABASE.get(_prefix(prefix), (None, {}))[1]
    _BIG_SEGMENT_DATABASE[_prefix(prefix)] = (metadata.last_up_to_date, memberships)


def _set_segments(prefix, context_hash, includes: List[str], excludes: List[str]):
    timestamp, memberships = _BIG_SEGMENT_DATABASE.get(_prefix(prefix), (None, {}))
    membership = {ref: False for ref in excludes}
    membership.update({ref: True for ref in includes})
    memberships[context_hash] = membership
    _BIG_SEGMENT_DATABASE[_prefix(prefix)] = (timestamp, memberships)


class _ReferenceCoreSuite(FeatureStoreCoreTestSuite):
    metadata_in_item_only = False

    def create_core(self, prefix):
        return _ReferenceCore(prefix, self.metadata_in_item_only)

    def clear_data(self, prefix):
        _clear(prefix)

    def create_error_core(self):
        core = _ReferenceCore(None, self.metadata_in_item_only)
        core.broken = True
        return core

    def set_concurrent_modification_hook(self, core, hook):
        core.hook = hook


class TestReferenceCore(_ReferenceCoreSuite):
    pass


class TestReferenceCoreWithMetadataInItemOnly(_ReferenceCoreSuite):
    metadata_in_item_only = True


class _AsyncReferenceCoreSuite(AsyncFeatureStoreCoreTestSuite):
    metadata_in_item_only = False

    def create_core(self, prefix):
        return _AsyncReferenceCore(prefix, self.metadata_in_item_only)

    async def clear_data(self, prefix):
        _clear(prefix)

    def create_error_core(self):
        core = _AsyncReferenceCore(None, self.metadata_in_item_only)
        core.broken = True
        return core

    def set_concurrent_modification_hook(self, core, hook):
        core.hook = hook


class TestAsyncReferenceCore(_AsyncReferenceCoreSuite):
    pass


class TestAsyncReferenceCoreWithMetadataInItemOnly(_AsyncReferenceCoreSuite):
    metadata_in_item_only = True


class TestReferenceBigSegmentStore(BigSegmentStoreTestSuite):
    def create_store(self, prefix):
        return _ReferenceBigSegmentStore(prefix)

    def clear_data(self, prefix):
        _clear(prefix)

    def set_metadata(self, prefix, metadata):
        _set_metadata(prefix, metadata)

    def set_segments(self, prefix, context_hash, includes, excludes):
        _set_segments(prefix, context_hash, includes, excludes)


class TestAsyncReferenceBigSegmentStore(AsyncBigSegmentStoreTestSuite):
    def create_store(self, prefix):
        return _AsyncReferenceBigSegmentStore(prefix)

    async def clear_data(self, prefix):
        _clear(prefix)

    async def set_metadata(self, prefix, metadata):
        _set_metadata(prefix, metadata)

    async def set_segments(self, prefix, context_hash, includes, excludes):
        _set_segments(prefix, context_hash, includes, excludes)


class _NoVersionCheckCore(_ReferenceCore):
    def upsert_internal(self, kind, key, item):
        self._items(kind)[key] = item
        return True


class _NoVersionCheckSuite(_ReferenceCoreSuite):
    def create_core(self, prefix):
        return _NoVersionCheckCore(prefix, False)


class _SinglePrefixSuite(_ReferenceCoreSuite):
    def create_core(self, prefix):
        return _ReferenceCore(None, False)


@pytest.mark.parametrize('test_name', [
    'test_upsert_older_version',
    'test_upsert_same_version',
    'test_delete_with_older_version',
    'test_upsert_race_against_a_higher_version',
])
def test_suite_detects_a_core_that_ignores_versions(test_name):
    with pytest.raises(AssertionError):
        getattr(_NoVersionCheckSuite(), test_name)()


@pytest.mark.parametrize('test_name', [
    'test_prefixes_are_independent_for_init',
    'test_prefixes_are_independent_for_upsert',
])
def test_suite_detects_a_core_that_ignores_the_prefix(test_name):
    with pytest.raises(AssertionError):
        getattr(_SinglePrefixSuite(), test_name)()
