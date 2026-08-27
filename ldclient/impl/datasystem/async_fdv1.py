from typing import Any, Callable, Dict, Optional

from ldclient.async_config import AsyncConfig
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.aio.transport import AsyncHTTPTransport, AsyncSSEFactory
from ldclient.impl.datasource.async_feature_requester import (
    AsyncFeatureRequesterImpl
)
from ldclient.impl.datasource.async_polling import AsyncPollingUpdateProcessor
from ldclient.impl.datasource.async_status import AsyncDataSourceUpdateSinkImpl
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingUpdateProcessor
)
from ldclient.impl.datasource.status import DataSourceStatusProviderImpl
from ldclient.impl.datastore.status import (
    DataStoreStatusProviderImpl,
    DataStoreUpdateSinkImpl
)
from ldclient.impl.datasystem import (
    AsyncDataSystem,
    DataAvailability,
    DiagnosticAccumulator
)
from ldclient.impl.datasystem.store import _decode
from ldclient.impl.listeners import Listeners
from ldclient.impl.stubs import AsyncNullUpdateProcessor
from ldclient.impl.util import log
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncReadOnlyStore,
    AsyncUpdateProcessor,
    DataSourceStatusProvider,
    DataStoreStatusProvider
)
from ldclient.versioned_data_kind import VersionedDataKind


class _AsyncReadOnlyFeatureStoreView(AsyncReadOnlyStore):
    """Read-only view of an async feature store.

    Serves every read from the wrapped store. Items that a custom feature store
    keeps as raw dicts are decoded into model objects; items that are already
    models pass through unchanged.
    """

    def __init__(self, store: AsyncReadOnlyStore):
        self._store = store

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        return _decode(kind, await self._store.get(kind, key))

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        result = await self._store.all(kind)
        return {key: _decode(kind, item) for key, item in result.items()}


class AsyncFDv1(AsyncDataSystem):
    """
    AsyncFDv1 provides the v1 data source and store behavior through the
    AsyncDataSystem interface. It is the async version of
    :class:`ldclient.impl.datasystem.fdv1.FDv1`. Unlike the sync side, it uses
    the feature store directly and does not wrap it for persistent-store status
    monitoring.
    """

    def __init__(self, config: AsyncConfig, store: AsyncFeatureStore, session_provider: Callable[[], Any]):
        self._config = config
        self._store = store
        self._store_view = _AsyncReadOnlyFeatureStoreView(store)
        # The client creates the aiohttp session lazily inside the loop; the data
        # source resolves it here when it builds its network processor at start().
        self._session_provider = session_provider

        # Set up data store status tracking (no store wrapper)
        self._data_store_listeners = Listeners()
        self._data_store_update_sink = DataStoreUpdateSinkImpl(
            self._data_store_listeners
        )
        # The provider only calls the store's monitoring methods, which the async
        # store also has, so the sync-typed signature is fine.
        self._data_store_status_provider_impl = DataStoreStatusProviderImpl(
            self._store, self._data_store_update_sink  # type: ignore[arg-type]
        )

        # Set up the data source status tracking and listeners
        self._data_source_listeners = Listeners()
        self._flag_change_listeners = Listeners()
        self._data_source_update_sink = AsyncDataSourceUpdateSinkImpl(
            self._store,
            self._data_source_listeners,
            self._flag_change_listeners,
        )
        self._data_source_status_provider_impl = DataSourceStatusProviderImpl(
            self._data_source_listeners, self._data_source_update_sink
        )

        # v1 processors read the sink from the config for status updates. The config
        # attribute is typed as the sync sink, but the async sink has the same methods.
        self._config._data_source_update_sink = self._data_source_update_sink  # type: ignore[assignment]

        # Update processor created in start(), because it needs the ready event
        self._update_processor: Optional[AsyncUpdateProcessor] = None

        # Diagnostic accumulator provided by client for streaming metrics
        self._diagnostic_accumulator: Optional[DiagnosticAccumulator] = None

    def start(self, set_on_ready: AsyncEvent):
        """
        Starts the v1 update processor and returns immediately. The provided
        event is set by the processor upon first successful initialization or
        upon permanent failure.
        """
        update_processor = self._make_update_processor(
            self._config, self._store, set_on_ready
        )
        self._update_processor = update_processor
        update_processor.start()

    async def stop(self):
        if self._update_processor is not None:
            await self._update_processor.stop()

    @property
    def store(self) -> AsyncReadOnlyStore:
        return self._store_view

    def set_diagnostic_accumulator(self, diagnostic_accumulator: DiagnosticAccumulator):
        """
        Sets the diagnostic accumulator for streaming initialization metrics.
        This should be called before start() to ensure metrics are collected.
        """
        self._diagnostic_accumulator = diagnostic_accumulator

    @property
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        return self._data_source_status_provider_impl

    @property
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        return self._data_store_status_provider_impl

    @property
    def flag_change_listeners(self) -> Listeners:
        return self._flag_change_listeners

    async def data_availability(self) -> DataAvailability:
        if self._config.offline:
            return DataAvailability.DEFAULTS

        if self._update_processor is not None and self._update_processor.initialized():
            return DataAvailability.REFRESHED

        # Awaits the store so a persistent store populated by another process is
        # recognized. A persistent-store error is logged and reported as no data.
        try:
            ready = await self._store.is_initialized()
        except Exception as e:
            log.warning("Error checking persistent store readiness: %s", e)
            return DataAvailability.DEFAULTS

        return DataAvailability.CACHED if ready else DataAvailability.DEFAULTS

    @property
    def target_availability(self) -> DataAvailability:
        if self._config.offline:
            return DataAvailability.DEFAULTS
        # In LDD mode or normal connected modes, the ideal is to be refreshed
        return DataAvailability.REFRESHED

    def _make_update_processor(self, config: AsyncConfig, store: AsyncFeatureStore, ready: AsyncEvent):
        # Mirrors FDv1._make_update_processor but builds the async processors
        if config.update_processor_class:
            log.info("Using user-specified update processor: " + str(config.update_processor_class))
            return config.update_processor_class(config, store, ready)

        if config.offline or config.use_ldd:
            return AsyncNullUpdateProcessor(config, store, ready)

        if config.stream:
            return AsyncStreamingUpdateProcessor(
                config,
                store,
                ready,
                self._diagnostic_accumulator,
                AsyncSSEFactory(config, session=self._session_provider(), proxy=config.http.http_proxy),
            )

        log.info("Disabling streaming API")
        log.warning("You should only disable the streaming API if instructed to do so by LaunchDarkly support")

        if config.feature_requester_class:
            feature_requester = config.feature_requester_class(config)
        else:
            feature_requester = AsyncFeatureRequesterImpl(
                config,
                AsyncHTTPTransport(config, client=self._session_provider()),
            )
        return AsyncPollingUpdateProcessor(config, feature_requester, store, ready)
