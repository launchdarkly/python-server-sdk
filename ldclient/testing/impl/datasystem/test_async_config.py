# pylint: disable=missing-docstring

from typing import Any, Dict, Mapping, Optional

from ldclient.async_config import AsyncDataSystemConfig
from ldclient.config import DataSystemConfig
from ldclient.impl.datasourcev2.async_polling import (
    AsyncFallbackToFDv1PollingDataSourceBuilder,
    AsyncPollingDataSourceBuilder
)
from ldclient.impl.datasourcev2.async_streaming import (
    AsyncStreamingDataSourceBuilder
)
from ldclient.interfaces import AsyncFeatureStore, DataStoreMode
from ldclient.versioned_data_kind import VersionedDataKind


class FakeAsyncStore(AsyncFeatureStore):
    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        pass

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        return None

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        return {}

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        return True

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        return True

    @property
    def initialized(self) -> bool:
        return True

    async def is_initialized(self) -> bool:
        return True


def test_async_data_system_config_defaults():
    cfg = AsyncDataSystemConfig()
    assert cfg.initializers is None
    assert cfg.synchronizers is None
    assert cfg.data_store is None
    assert cfg.fdv1_fallback_synchronizer is None
    # Reuses the shared DataStoreMode enum with the same default as the sync config.
    assert cfg.data_store_mode is DataStoreMode.READ_WRITE
    assert cfg.data_store_mode is DataSystemConfig.data_store_mode


def test_async_data_system_config_accepts_async_builders_and_store():
    store = FakeAsyncStore()
    cfg = AsyncDataSystemConfig(
        initializers=[AsyncPollingDataSourceBuilder()],
        synchronizers=[AsyncStreamingDataSourceBuilder(), AsyncPollingDataSourceBuilder()],
        data_store_mode=DataStoreMode.READ_ONLY,
        data_store=store,
        fdv1_fallback_synchronizer=AsyncFallbackToFDv1PollingDataSourceBuilder(),
    )

    assert cfg.initializers is not None and len(cfg.initializers) == 1
    assert cfg.synchronizers is not None and len(cfg.synchronizers) == 2
    assert cfg.data_store is store
    assert isinstance(cfg.data_store, AsyncFeatureStore)
    assert cfg.data_store_mode is DataStoreMode.READ_ONLY
    assert cfg.fdv1_fallback_synchronizer is not None


def test_async_data_system_config_shares_data_store_mode_enum():
    # The async config does not define its own mode enum.
    assert AsyncDataSystemConfig(data_store_mode=DataStoreMode.READ_WRITE).data_store_mode \
        is DataSystemConfig(initializers=None, synchronizers=None, data_store_mode=DataStoreMode.READ_WRITE).data_store_mode
