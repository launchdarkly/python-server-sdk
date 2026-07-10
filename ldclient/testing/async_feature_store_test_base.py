"""
Async equivalent of feature_store_test_base.py. Provides a reusable test suite that can be
run against any AsyncFeatureStore implementation.
"""
from abc import abstractmethod

import pytest

from ldclient.interfaces import AsyncFeatureStore
from ldclient.testing.builders import FlagBuilder
from ldclient.versioned_data_kind import FEATURES


class AsyncFeatureStoreTester:
    @abstractmethod
    async def create_feature_store(self) -> AsyncFeatureStore:
        pass


class AsyncFeatureStoreTestBase:
    """Base class for async feature store tests.

    Subclasses must provide a pytest fixture called ``tester`` that returns an instance of
    ``AsyncFeatureStoreTester``. Each test method receives the tester as a parameter and creates
    its own store instance to ensure test isolation.
    """

    @abstractmethod
    def all_testers(self):
        pass

    @staticmethod
    def make_feature(key, ver):
        return FlagBuilder(key).version(ver).on(True).variations(True, False).salt('abc').build()

    async def make_inited_store(self, tester) -> AsyncFeatureStore:
        store = await tester.create_feature_store()
        await store.init(
            {
                FEATURES: {
                    'foo': self.make_feature('foo', 10).to_json_dict(),
                    'bar': self.make_feature('bar', 10).to_json_dict(),
                }
            }
        )
        return store

    @pytest.mark.asyncio
    async def test_not_initialized_before_init(self, tester):
        store = await tester.create_feature_store()
        assert store.initialized is False

    @pytest.mark.asyncio
    async def test_initialized(self, tester):
        store = await self.make_inited_store(tester)
        assert store.initialized is True

    @pytest.mark.asyncio
    async def test_get_existing_feature(self, tester):
        store = await self.make_inited_store(tester)
        expected = self.make_feature('foo', 10)
        flag = await store.get(FEATURES, 'foo')
        assert flag == expected

    @pytest.mark.asyncio
    async def test_get_nonexisting_feature(self, tester):
        store = await self.make_inited_store(tester)
        assert await store.get(FEATURES, 'biz') is None

    @pytest.mark.asyncio
    async def test_get_all_versions(self, tester):
        store = await self.make_inited_store(tester)
        result = await store.all(FEATURES)
        assert len(result) == 2
        assert result.get('foo') == self.make_feature('foo', 10)
        assert result.get('bar') == self.make_feature('bar', 10)

    @pytest.mark.asyncio
    async def test_upsert_with_newer_version(self, tester):
        store = await self.make_inited_store(tester)
        new_ver = self.make_feature('foo', 11)
        await store.upsert(FEATURES, new_ver)
        assert await store.get(FEATURES, 'foo') == new_ver

    @pytest.mark.asyncio
    async def test_upsert_with_older_version(self, tester):
        store = await self.make_inited_store(tester)
        new_ver = self.make_feature('foo', 9)
        expected = self.make_feature('foo', 10)
        await store.upsert(FEATURES, new_ver)
        assert await store.get(FEATURES, 'foo') == expected

    @pytest.mark.asyncio
    async def test_upsert_with_new_feature(self, tester):
        store = await self.make_inited_store(tester)
        new_ver = self.make_feature('biz', 1)
        await store.upsert(FEATURES, new_ver)
        assert await store.get(FEATURES, 'biz') == new_ver

    @pytest.mark.asyncio
    async def test_delete_with_newer_version(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'foo', 11)
        assert await store.get(FEATURES, 'foo') is None

    @pytest.mark.asyncio
    async def test_delete_unknown_feature(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'biz', 11)
        assert await store.get(FEATURES, 'biz') is None

    @pytest.mark.asyncio
    async def test_delete_with_older_version(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'foo', 9)
        expected = self.make_feature('foo', 10)
        assert await store.get(FEATURES, 'foo') == expected

    @pytest.mark.asyncio
    async def test_upsert_older_version_after_delete(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'foo', 11)
        old_ver = self.make_feature('foo', 9)
        await store.upsert(FEATURES, old_ver)
        assert await store.get(FEATURES, 'foo') is None

    @pytest.mark.asyncio
    async def test_deleted_item_not_in_all(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'foo', 11)
        result = await store.all(FEATURES)
        assert 'foo' not in result
        assert 'bar' in result

    @pytest.mark.asyncio
    async def test_upsert_with_equal_version_not_applied(self, tester):
        store = await self.make_inited_store(tester)
        same_ver = self.make_feature('foo', 10)
        expected = self.make_feature('foo', 10)
        await store.upsert(FEATURES, same_ver)
        assert await store.get(FEATURES, 'foo') == expected

    @pytest.mark.asyncio
    async def test_delete_with_equal_version_not_applied(self, tester):
        store = await self.make_inited_store(tester)
        await store.delete(FEATURES, 'foo', 10)
        expected = self.make_feature('foo', 10)
        assert await store.get(FEATURES, 'foo') == expected

    @pytest.mark.asyncio
    async def test_init_replaces_existing_data(self, tester):
        store = await tester.create_feature_store()
        await store.upsert(FEATURES, self.make_feature('foo', 1))
        await store.init(
            {
                FEATURES: {
                    'bar': self.make_feature('bar', 1).to_json_dict(),
                }
            }
        )
        assert await store.get(FEATURES, 'foo') is None
        assert await store.get(FEATURES, 'bar') == self.make_feature('bar', 1)
