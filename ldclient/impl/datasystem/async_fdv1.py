from typing import Any, Optional

from ldclient.async_config import AsyncConfig
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.aio.transport import AsyncHTTPTransport, AsyncSSEFactory
from ldclient.impl.datasource.async_feature_requester import (
    AsyncFeatureRequester
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
from ldclient.impl.listeners import Listeners
from ldclient.impl.stubs import AsyncNullUpdateProcessor
from ldclient.impl.util import log
from ldclient.interfaces import (
    AsyncFeatureStore,
    AsyncReadOnlyStore,
    DataSourceStatusProvider,
    DataStoreStatusProvider
)


class AsyncFDv1(AsyncDataSystem):
    """
    AsyncFDv1 wires the v1 async data source and store behavior behind the
    generic AsyncDataSystem surface. It is the async twin of
    :class:`ldclient.impl.datasystem.fdv1.FDv1`; unlike the sync side, the
    feature store is used directly (async stores are not wrapped for
    persistent-store status monitoring).
    """

    def __init__(self, config: AsyncConfig, store: AsyncFeatureStore, session: Optional[Any] = None, proxy: Optional[str] = None):
        self._config = config
        self._store = store
        self._session = session
        self._proxy = proxy

        # Set up data store plumbing (status tracking only — no wrapper)
        self._data_store_listeners = Listeners()
        self._data_store_update_sink = DataStoreUpdateSinkImpl(
            self._data_store_listeners
        )
        # The provider only uses duck-typed monitoring methods, so an async
        # store works despite the sync-typed signature.
        self._data_store_status_provider_impl = DataStoreStatusProviderImpl(
            self._store, self._data_store_update_sink  # type: ignore[arg-type]
        )

        # Set up data source plumbing
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

        # Ensure v1 processors can find the sink via config for status updates
        # (the config attribute is sync-typed; the async sink is its duck-typed twin)
        self._config._data_source_update_sink = self._data_source_update_sink  # type: ignore[assignment]

        # Update processor created in start(), because it needs the ready event
        self._update_processor: Optional[Any] = None

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
        return self._store

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

    @property
    def data_availability(self) -> DataAvailability:
        if self._config.offline:
            return DataAvailability.DEFAULTS

        if self._update_processor is not None and self._update_processor.initialized():
            return DataAvailability.REFRESHED

        if self._store.initialized:
            return DataAvailability.CACHED

        return DataAvailability.DEFAULTS

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
            # The config attribute is sync-typed; async clients supply a
            # processor class built for async stores and the AsyncEvent shim.
            return config.update_processor_class(config, store, ready)  # type: ignore[arg-type]

        if config.offline or config.use_ldd:
            return AsyncNullUpdateProcessor(config, store, ready)

        if config.stream:
            return AsyncStreamingUpdateProcessor(
                config,
                store,
                ready,
                self._diagnostic_accumulator,
                AsyncSSEFactory(config, session=self._session, proxy=self._proxy),
            )

        log.info("Disabling streaming API")
        log.warning("You should only disable the streaming API if instructed to do so by LaunchDarkly support")

        if config.feature_requester_class:
            feature_requester = config.feature_requester_class(config)
        else:
            feature_requester = AsyncFeatureRequester(
                config,
                AsyncHTTPTransport(config, client=self._session),
            )
        return AsyncPollingUpdateProcessor(config, feature_requester, store, ready)
