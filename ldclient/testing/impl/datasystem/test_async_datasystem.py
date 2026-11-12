from types import SimpleNamespace
from unittest import mock

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.async_datasystem import (
    AsyncConfigBuilder,
    custom,
    daemon,
    default,
    fdv1_fallback_ds_builder,
    persistent_store,
    polling,
    polling_ds_builder,
    streaming,
    streaming_ds_builder
)
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.context import Context
from ldclient.impl.datasource.async_polling import (
    AsyncFallbackToFDv1PollingDataSourceBuilder,
    AsyncPollingDataSourceBuilder
)
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingDataSourceBuilder
)
from ldclient.integrations.test_data import TestData
from ldclient.interfaces import DataStoreMode


def test_empty_builder_builds_an_empty_config():
    config = AsyncConfigBuilder().build()

    assert config == AsyncDataSystemConfig(
        initializers=None,
        synchronizers=None,
        fdv1_fallback_synchronizer=None,
        data_store_mode=DataStoreMode.READ_ONLY,
        data_store=None,
    )


def test_builder_sets_every_field():
    initializer = polling_ds_builder()
    primary = streaming_ds_builder()
    secondary = polling_ds_builder()
    fallback = fdv1_fallback_ds_builder()
    store = AsyncInMemoryFeatureStore()

    config = (
        AsyncConfigBuilder()
        .initializers([initializer])
        .synchronizers(primary, secondary)
        .fdv1_compatible_synchronizer(fallback)
        .data_store(store, DataStoreMode.READ_WRITE)
        .build()
    )

    assert config.initializers == [initializer]
    assert config.synchronizers == [primary, secondary]
    assert config.fdv1_fallback_synchronizer is fallback
    assert config.data_store is store
    assert config.data_store_mode == DataStoreMode.READ_WRITE


def test_synchronizers_needs_at_least_one_builder():
    with pytest.raises(ValueError):
        AsyncConfigBuilder().synchronizers()


def test_custom_returns_a_new_empty_builder():
    first = custom()
    second = custom()

    assert first is not second
    assert first.build() == AsyncConfigBuilder().build()


def test_default_polls_first_then_streams_with_polling_and_fdv1_fallbacks():
    config = default().build()

    assert len(config.initializers) == 1
    assert isinstance(config.initializers[0], AsyncPollingDataSourceBuilder)
    assert len(config.synchronizers) == 2
    assert isinstance(config.synchronizers[0], AsyncStreamingDataSourceBuilder)
    assert isinstance(config.synchronizers[1], AsyncPollingDataSourceBuilder)
    assert isinstance(config.fdv1_fallback_synchronizer, AsyncFallbackToFDv1PollingDataSourceBuilder)
    assert config.data_store is None


def test_streaming_streams_with_an_fdv1_fallback():
    config = streaming().build()

    assert config.initializers is None
    assert len(config.synchronizers) == 1
    assert isinstance(config.synchronizers[0], AsyncStreamingDataSourceBuilder)
    assert isinstance(config.fdv1_fallback_synchronizer, AsyncFallbackToFDv1PollingDataSourceBuilder)


def test_polling_polls_with_an_fdv1_fallback():
    config = polling().build()

    assert config.initializers is None
    assert len(config.synchronizers) == 1
    assert isinstance(config.synchronizers[0], AsyncPollingDataSourceBuilder)
    assert isinstance(config.fdv1_fallback_synchronizer, AsyncFallbackToFDv1PollingDataSourceBuilder)


def test_daemon_reads_only_from_the_store():
    store = AsyncInMemoryFeatureStore()
    config = daemon(store).build()

    assert config.initializers is None
    assert config.synchronizers is None
    assert config.fdv1_fallback_synchronizer is None
    assert config.data_store is store
    assert config.data_store_mode == DataStoreMode.READ_ONLY


def test_persistent_store_is_the_default_with_a_writable_store():
    store = AsyncInMemoryFeatureStore()
    config = persistent_store(store).build()

    assert isinstance(config.initializers[0], AsyncPollingDataSourceBuilder)
    assert isinstance(config.synchronizers[0], AsyncStreamingDataSourceBuilder)
    assert isinstance(config.fdv1_fallback_synchronizer, AsyncFallbackToFDv1PollingDataSourceBuilder)
    assert config.data_store is store
    assert config.data_store_mode == DataStoreMode.READ_WRITE


def test_client_gives_its_session_to_every_builder_of_the_default_config():
    # The async client shares one aiohttp session with each data source that the default
    # strategy builds.
    config = default().build()
    session = object()
    client = SimpleNamespace(_get_session=lambda: session)

    with mock.patch.object(AsyncPollingDataSourceBuilder, 'session') as polling_session, \
            mock.patch.object(AsyncStreamingDataSourceBuilder, 'session') as streaming_session, \
            mock.patch.object(AsyncFallbackToFDv1PollingDataSourceBuilder, 'session') as fallback_session:
        AsyncLDClient._wire_data_source_sessions(client, config)

    # The polling builder is the initializer and also the second synchronizer.
    assert polling_session.call_count == 2
    assert streaming_session.call_count == 1
    assert fallback_session.call_count == 1
    for patched in (polling_session, streaming_session, fallback_session):
        for call in patched.call_args_list:
            assert call.args == (session,)


@pytest.mark.asyncio
async def test_client_evaluates_flags_from_a_custom_config():
    td = TestData.data_source()
    td.update(td.flag('flag').on(True))
    data_system = custom().synchronizers(td.async_builder).build()
    config = AsyncConfig('sdk-key', send_events=False, data_system=data_system)

    async with AsyncLDClient(config) as client:
        assert await client.is_initialized()
        assert await client.variation('flag', Context.create('user'), False) is True
