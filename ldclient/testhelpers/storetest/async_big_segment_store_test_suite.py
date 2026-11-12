from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional

import pytest

from ldclient.interfaces import AsyncBigSegmentStore, BigSegmentStoreMetadata

_CONTEXT_HASH = 'contexthash'


class AsyncBigSegmentStoreTestSuite(ABC):
    """
    A conformance test suite for implementations of :class:`ldclient.interfaces.AsyncBigSegmentStore`.

    The suite runs with pytest. To use it, define a test class that extends this class and
    implements the abstract methods. The suite runs each test with the default prefix of the store
    and with a specified prefix.
    """

    @abstractmethod
    def create_store(self, prefix: Optional[str]) -> AsyncBigSegmentStore:
        """
        Returns a new store that uses the given prefix.

        :param prefix: the prefix for the store data, or None to use the default prefix of the store
        """

    @abstractmethod
    async def clear_data(self, prefix: Optional[str]) -> None:
        """
        Deletes all data in the database for the given prefix.
        """

    @abstractmethod
    async def set_metadata(self, prefix: Optional[str], metadata: BigSegmentStoreMetadata) -> None:
        """
        Writes the store metadata to the database, as the Relay Proxy does.
        """

    @abstractmethod
    async def set_segments(self, prefix: Optional[str], context_hash: str, includes: List[str], excludes: List[str]) -> None:
        """
        Writes the segment membership of a context to the database, as the Relay Proxy does.
        """

    @pytest.fixture(params=[None, 'testprefix'])
    def prefix(self, request) -> Optional[str]:
        return request.param

    @asynccontextmanager
    async def _store(self, prefix: Optional[str]) -> AsyncIterator[AsyncBigSegmentStore]:
        store = self.create_store(prefix)
        try:
            yield store
        finally:
            await store.stop()

    @pytest.mark.asyncio
    async def test_get_metadata_valid_value(self, prefix):
        await self.clear_data(prefix)
        await self.set_metadata(prefix, BigSegmentStoreMetadata(1234567890))
        async with self._store(prefix) as store:
            metadata = await store.get_metadata()
            assert metadata is not None
            assert metadata.last_up_to_date == 1234567890

    @pytest.mark.asyncio
    async def test_get_metadata_no_value(self, prefix):
        await self.clear_data(prefix)
        async with self._store(prefix) as store:
            metadata = await store.get_metadata()
            assert metadata is not None
            assert metadata.last_up_to_date is None

    @pytest.mark.asyncio
    async def test_get_membership_not_found(self, prefix):
        await self.clear_data(prefix)
        async with self._store(prefix) as store:
            membership = await store.get_membership(_CONTEXT_HASH)
            assert membership is None or membership == {}

    @pytest.mark.asyncio
    async def test_get_membership_includes_only(self, prefix):
        await self.clear_data(prefix)
        await self.set_segments(prefix, _CONTEXT_HASH, ['key1', 'key2'], [])
        async with self._store(prefix) as store:
            assert await store.get_membership(_CONTEXT_HASH) == {'key1': True, 'key2': True}

    @pytest.mark.asyncio
    async def test_get_membership_excludes_only(self, prefix):
        await self.clear_data(prefix)
        await self.set_segments(prefix, _CONTEXT_HASH, [], ['key1', 'key2'])
        async with self._store(prefix) as store:
            assert await store.get_membership(_CONTEXT_HASH) == {'key1': False, 'key2': False}

    @pytest.mark.asyncio
    async def test_get_membership_includes_and_excludes(self, prefix):
        await self.clear_data(prefix)
        # An include takes precedence over an exclude for the same segment.
        await self.set_segments(prefix, _CONTEXT_HASH, ['key1', 'key2'], ['key2', 'key3'])
        async with self._store(prefix) as store:
            assert await store.get_membership(_CONTEXT_HASH) == {'key1': True, 'key2': True, 'key3': False}
