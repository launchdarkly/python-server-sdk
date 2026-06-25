"""
Async client for the LaunchDarkly Server-Side Python SDK.
"""

import asyncio
import traceback
from typing import Any, Callable, List, Optional, Tuple
from uuid import uuid4

import certifi

from ldclient.async_config import AsyncConfig
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail, FeatureFlagsState
from ldclient.hook import (
    AsyncHook,
    EvaluationSeriesContext,
    _EvaluationWithHookResult
)
from ldclient.impl import AnyNum
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.async_big_segments import AsyncBigSegmentStoreManager
from ldclient.impl.async_evaluator import AsyncEvaluator, error_reason
from ldclient.impl.async_flag_tracker import AsyncFlagTrackerImpl
from ldclient.impl.client_common import (
    get_environment_metadata,
    get_plugin_hooks
)
from ldclient.impl.client_common import secure_mode_hash as _secure_mode_hash
from ldclient.impl.datasystem import AsyncDataSystem, DataAvailability
from ldclient.impl.events.async_event_processor import AsyncEventProcessor
from ldclient.impl.events.diagnostics import (
    _DiagnosticAccumulator,
    create_diagnostic_id
)
from ldclient.impl.events.types import EventFactory
from ldclient.impl.model.feature_flag import FeatureFlag
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.stubs import AsyncNullEventProcessor
from ldclient.impl.util import log
from ldclient.interfaces import (
    AsyncFeatureStore,
    BigSegmentStoreStatusProvider,
    DataSourceStatusProvider,
    DataStoreStatusProvider,
    FlagTracker
)
from ldclient.migrations import OpTracker, Stage
from ldclient.plugin import EnvironmentMetadata
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


async def _get_store_item(store, kind: VersionedDataKind, key: str) -> Any:
    # This decorator around store.get provides backward compatibility with any custom data
    # store implementation that might still be returning a dict, instead of our data model
    # classes like FeatureFlag.
    item = await store.get(kind, key)
    return kind.decode(item) if isinstance(item, dict) else item


class _NotStartedDataSystem:
    """Placeholder data system used before start(); reports that only
    application-provided defaults are available."""

    @property
    def data_availability(self) -> DataAvailability:
        return DataAvailability.DEFAULTS

    async def stop(self) -> None:
        pass


class AsyncLDClient:
    """Async LaunchDarkly SDK client.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    Use ``async with AsyncLDClient(config) as client:`` or call
    ``await client.start()`` / ``await client.close()`` explicitly.
    """

    def __init__(self, config: AsyncConfig):
        """
        Construct an AsyncLDClient.  Does NOT start background tasks; call
        ``await start()`` (or use the async context manager) before evaluating flags.

        :param config: SDK configuration
        """
        config._validate()

        self._config = config
        self._config._instance_id = str(uuid4())
        self._lifecycle_lock = asyncio.Lock()

        self._started = False
        self._closed = False

        self._session = None
        self._proxy: Optional[str] = None
        # Pre-start placeholders so that evaluation/track/identify before
        # start() degrade gracefully (defaults returned, events dropped).
        self._event_processor: Any = AsyncNullEventProcessor()
        self._data_system: AsyncDataSystem = _NotStartedDataSystem()  # type: ignore[assignment]

        self.__hooks_lock = ReadWriteLock()
        self.__hooks: List = list(config.hooks)

        self._event_factory_default = EventFactory(False)
        self._event_factory_with_reasons = EventFactory(True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self, start_wait: float = 5.0) -> None:
        """Start the client: create the HTTP session, data system, and event processor.

        Safe to call multiple times — subsequent calls are no-ops.

        :param start_wait: seconds to wait for the data source to initialize
        """
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("Cannot start a closed AsyncLDClient")
            if self._started:
                return

            # __start_up resets the hook list to config.hooks + plugin hooks;
            # preserve any hooks registered via add_hook() before start().
            with self.__hooks_lock.read():
                pre_start_hooks = [h for h in self.__hooks if h not in self._config.hooks]

            try:
                await self.__start_up(start_wait)
                self._started = True
            except Exception:
                await self._cleanup_partial_start()
                raise

            for hook in pre_start_hooks:
                self.add_hook(hook)

    async def _cleanup_partial_start(self):
        """Release any resources that were partially created during a failed __start_up."""
        try:
            await self._event_processor.stop()
        except Exception:
            pass
        try:
            await self._data_system.stop()
        except Exception:
            pass
        try:
            manager = self.__big_segment_store_manager
        except AttributeError:
            manager = None
        if manager is not None:
            try:
                await manager.stop()
            except Exception:
                pass
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass

    async def close(self, close_timeout: float = 2.0) -> None:
        """Shut down the client and release all resources.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True

            if self._started:
                try:
                    await asyncio.wait_for(self._close_components(), timeout=close_timeout)
                except asyncio.TimeoutError:
                    log.warning("Timed out closing AsyncLDClient components")
                except Exception as e:
                    log.warning("Error closing AsyncLDClient components: %s", e)

            # Close HTTP session
            if self._session is not None:
                try:
                    await self._session.close()
                except Exception as e:
                    log.warning("Error closing HTTP session: %s", e)

    async def _close_components(self):
        """Releases the threads and network connections used by the SDK
        components. The public :meth:`close` wraps this with a timeout."""
        log.info("Closing LaunchDarkly client..")
        await self._event_processor.stop()
        await self._data_system.stop()
        await self.__big_segment_store_manager.stop()

    # ------------------------------------------------------------------
    # Start wiring
    # ------------------------------------------------------------------

    async def __start_up(self, start_wait: float):
        environment_metadata = get_environment_metadata(self._config, "python-server-sdk-async")
        plugin_hooks = get_plugin_hooks(self._config.plugins, environment_metadata)

        self.__hooks_lock = ReadWriteLock()
        self.__hooks = self._config.hooks + plugin_hooks

        self._session = await self._create_http_session()
        self._data_system = self._make_data_system()

        async def variation_eval_fn(key, context):
            return await self.variation(key, context, None)

        self.__flag_tracker = AsyncFlagTrackerImpl(
            self._data_system.flag_change_listeners,
            variation_eval_fn
        )
        # Expose providers and store from data system
        self.__data_store_status_provider = self._data_system.data_store_status_provider
        self.__data_source_status_provider = (
            self._data_system.data_source_status_provider
        )

        big_segment_store_manager = AsyncBigSegmentStoreManager(self._config.big_segments)
        self.__big_segment_store_manager = big_segment_store_manager

        async def get_flag_fn(key):
            return await _get_store_item(self._data_system.store, FEATURES, key)

        async def get_segment_fn(key):
            return await _get_store_item(self._data_system.store, SEGMENTS, key)

        async def get_membership_fn(key):
            return await big_segment_store_manager.get_user_membership(key)

        self._evaluator = AsyncEvaluator(
            get_flag_fn,
            get_segment_fn,
            get_membership_fn,
            log,
        )

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")

        if self._config.use_ldd:
            log.info("Started LaunchDarkly Client in LDD mode")

        diagnostic_accumulator = self._set_event_processor(self._config)

        # Pass diagnostic accumulator to data system for streaming metrics
        self._data_system.set_diagnostic_accumulator(diagnostic_accumulator)  # type: ignore

        await self.__register_plugins(environment_metadata)

        update_processor_ready = AsyncEvent()
        self._data_system.start(update_processor_ready)

        if not self._config.offline and not self._config.use_ldd:
            if start_wait > 60:
                log.warning(f"Client was configured to block for up to {start_wait} seconds when initializing. We recommend blocking no longer than 60.")

            if start_wait > 0:
                log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
                await update_processor_ready.wait(start_wait)

        if self.is_initialized() is True:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.warning("Initialization timeout exceeded for LaunchDarkly Client or an error occurred. " "Feature Flags may not yet be available.")

    async def _create_http_session(self):
        """Create and return the aiohttp session. Called from __start_up."""
        import ssl

        import aiohttp

        ssl_ctx = ssl.create_default_context(
            cafile=self._config.http.ca_certs or certifi.where()
        )
        if self._config.http.cert_file:
            ssl_ctx.load_cert_chain(self._config.http.cert_file)
        if self._config.http.disable_ssl_verification:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            log.warning("TLS verification disabled")

        connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit_per_host=10)
        self._proxy = self._config.http.http_proxy
        return aiohttp.ClientSession(
            connector=connector,
            trust_env=(self._proxy is None),
        )

    def _make_data_system(self) -> AsyncDataSystem:
        datasystem_config = self._config.datasystem_config
        if datasystem_config is None:
            from ldclient.impl.datasystem.async_fdv1 import AsyncFDv1

            return AsyncFDv1(self._config, self._select_feature_store(), self._session, self._proxy)

        from ldclient.impl.datasystem.async_fdv2 import AsyncFDv2

        self._wire_data_source_sessions(datasystem_config)
        return AsyncFDv2(self._config, datasystem_config)

    def _select_feature_store(self) -> AsyncFeatureStore:
        """Choose the async feature store for the v1 data system based on the
        configured store."""
        feature_store = self._config.feature_store
        if feature_store is None:
            return AsyncInMemoryFeatureStore()
        return feature_store

    def _wire_data_source_sessions(self, data_system_config) -> None:
        """Provide the client's aiohttp session to any async data source
        builders so the sources they build share the client's connection pool."""
        from ldclient.impl.datasourcev2.async_polling import (
            AsyncFallbackToFDv1PollingDataSourceBuilder,
            AsyncPollingDataSourceBuilder
        )
        from ldclient.impl.datasourcev2.async_streaming import (
            AsyncStreamingDataSourceBuilder
        )

        builders = list(data_system_config.initializers or []) + list(
            data_system_config.synchronizers or []
        )
        if data_system_config.fdv1_fallback_synchronizer is not None:
            builders.append(data_system_config.fdv1_fallback_synchronizer)

        for builder in builders:
            if isinstance(
                builder,
                (
                    AsyncFallbackToFDv1PollingDataSourceBuilder,
                    AsyncPollingDataSourceBuilder,
                    AsyncStreamingDataSourceBuilder,
                ),
            ):
                builder.session(self._session)

    async def __register_plugins(self, environment_metadata: EnvironmentMetadata):
        for plugin in self._config.plugins:
            try:
                await plugin.register(self, environment_metadata)
            except Exception as e:
                log.error("Error registering plugin %s: %s", plugin.metadata.name, e)

    def _set_event_processor(self, config):
        if config.offline or not config.send_events:
            self._event_processor = AsyncNullEventProcessor()
            return None
        if not config.event_processor_class:
            diagnostic_id = create_diagnostic_id(config)
            diagnostic_accumulator = None if config.diagnostic_opt_out else _DiagnosticAccumulator(diagnostic_id)
            self._event_processor = AsyncEventProcessor(config, self._session, diagnostic_accumulator=diagnostic_accumulator)
            return diagnostic_accumulator
        self._event_processor = config.event_processor_class(config)
        return None

    # ------------------------------------------------------------------
    # SDK surface (I/O-adjacent — hand-duplicated with ldclient.client.LDClient,
    # differing only in async/await)
    # ------------------------------------------------------------------

    def get_sdk_key(self) -> Optional[str]:
        """Returns the configured SDK key."""
        return self._config.sdk_key

    def _send_event(self, event):
        self._event_processor.send_event(event)

    def track_migration_op(self, tracker: OpTracker):
        """
        Tracks the results of a migrations operation. This event includes
        measurements which can be used to enhance the observability of a
        migration within the LaunchDarkly UI.

        Customers making use of the :class:`ldclient.MigrationBuilder` should
        not need to call this method manually.

        Customers not using the builder should provide this method with the
        tracker returned from calling :func:`migration_variation`.
        """
        event = tracker.build()

        if isinstance(event, str):
            log.error("error generting migration op event %s; no event will be emitted", event)
            return

        self._send_event(event)

    def track(self, event_name: str, context: Context, data: Optional[Any] = None, metric_value: Optional[AnyNum] = None):
        """Tracks that an application-defined event occurred.

        This method creates a "custom" analytics event containing the specified event name (key)
        and context properties. You may attach arbitrary data or a metric value to the event with the
        optional ``data`` and ``metric_value`` parameters.

        Note that event delivery is asynchronous, so the event may not actually be sent until later;
        see :func:`flush()`.

        :param event_name: the name of the event
        :param context: the evaluation context associated with the event
        :param data: optional additional data associated with the event
        :param metric_value: a numeric value used by the LaunchDarkly experimentation feature in
          numeric custom metrics; can be omitted if this event is used by only non-numeric metrics
        """
        if not context.valid:
            log.warning("Invalid context for track (%s)" % context.error)
        else:
            self._send_event(self._event_factory_default.new_custom_event(event_name, context, data, metric_value))

    def identify(self, context: Context):
        """Reports details about an evaluation context.

        This method simply creates an analytics event containing the context properties, to
        that LaunchDarkly will know about that context if it does not already.

        Evaluating a flag, by calling :func:`variation()` or :func:`variation_detail()`, also
        sends the context information to LaunchDarkly (if events are enabled), so you only
        need to use :func:`identify()` if you want to identify the context without evaluating a
        flag.

        :param context: the context to register
        """

        if not context.valid:
            log.warning("Invalid context for identify (%s)" % context.error)
        else:
            self._send_event(self._event_factory_default.new_identify_event(context))

    def is_offline(self) -> bool:
        """Returns true if the client is in offline mode."""
        return self._config.offline

    def is_initialized(self) -> bool:
        """Returns true if the client has successfully connected to LaunchDarkly.

        If this returns false, it means that the client has not yet successfully connected to LaunchDarkly.
        It might still be in the process of starting up, or it might be attempting to reconnect after an
        unsuccessful attempt, or it might have received an unrecoverable error (such as an invalid SDK key)
        and given up.
        """
        if self.is_offline() or self._config.use_ldd:
            return True

        return self._data_system.data_availability.at_least(DataAvailability.CACHED)

    async def flush(self):
        """Flushes all pending analytics events.

        Normally, batches of events are delivered in the background at intervals determined by the
        ``flush_interval`` property of :class:`ldclient.config.Config`. Calling ``flush()``
        schedules the next event delivery to be as soon as possible; however, the delivery still
        happens asynchronously on a worker thread, so this method will return immediately.
        """
        if self._config.offline:
            return
        # flush() only schedules delivery; it does not await, so there is
        # nothing to await here.
        self._event_processor.flush()

    async def variation(self, key: str, context: Context, default: Any) -> Any:
        """Calculates the value of a feature flag for a given context.

        :param key: the unique key for the feature flag
        :param context: the evaluation context
        :param default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: the variation for the given context, or the ``default`` value if the flag cannot be evaluated
        """

        async def evaluate():
            detail, _ = await self._evaluate_internal(key, context, default, self._event_factory_default)
            return _EvaluationWithHookResult(evaluation_detail=detail)

        return (await self.__evaluate_with_hooks(key=key, context=context, default_value=default, method="variation", block=evaluate)).evaluation_detail.value

    async def variation_detail(self, key: str, context: Context, default: Any) -> EvaluationDetail:
        """Calculates the value of a feature flag for a given context, and returns an object that
        describes the way the value was determined.

        The ``reason`` property in the result will also be included in analytics events, if you are
        capturing detailed event data for this flag.

        :param key: the unique key for the feature flag
        :param context: the evaluation context
        :param default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: an :class:`ldclient.evaluation.EvaluationDetail` object that includes the feature
          flag value and evaluation reason
        """

        async def evaluate():
            detail, _ = await self._evaluate_internal(key, context, default, self._event_factory_with_reasons)
            return _EvaluationWithHookResult(evaluation_detail=detail)

        return (await self.__evaluate_with_hooks(key=key, context=context, default_value=default, method="variation_detail", block=evaluate)).evaluation_detail

    async def migration_variation(self, key: str, context: Context, default_stage: Stage) -> Tuple[Stage, OpTracker]:
        """
        This method returns the migration stage of the migration feature flag
        for the given evaluation context.

        This method returns the default stage if there is an error or the flag
        does not exist. If the default stage is not a valid stage, then a
        default stage of :class:`ldclient.migrations.Stage.OFF` will be used
        instead.
        """
        if not isinstance(default_stage, Stage) or default_stage not in Stage:
            log.error(f"default stage {default_stage} is not a valid stage; using 'off' instead")
            default_stage = Stage.OFF

        async def evaluate():
            detail, flag = await self._evaluate_internal(key, context, default_stage.value, self._event_factory_default)

            if isinstance(detail.value, str):
                stage = Stage.from_str(detail.value)
                if stage is not None:
                    tracker = OpTracker(key, flag, context, detail, default_stage)
                    return _EvaluationWithHookResult(evaluation_detail=detail, results={'default_stage': stage, 'tracker': tracker})

            detail = EvaluationDetail(default_stage.value, None, error_reason('WRONG_TYPE'))
            tracker = OpTracker(key, flag, context, detail, default_stage)
            return _EvaluationWithHookResult(evaluation_detail=detail, results={'default_stage': default_stage, 'tracker': tracker})

        hook_result = await self.__evaluate_with_hooks(key=key, context=context, default_value=default_stage.value, method="migration_variation", block=evaluate)
        return hook_result.results['default_stage'], hook_result.results['tracker']

    async def _evaluate_internal(self, key: str, context: Context, default: Any, event_factory) -> Tuple[EvaluationDetail, Optional[FeatureFlag]]:
        default = self._config.get_default(key, default)

        if self._config.offline:
            return EvaluationDetail(default, None, error_reason('CLIENT_NOT_READY')), None

        if self._data_system.data_availability != DataAvailability.REFRESHED:
            if self._data_system.data_availability == DataAvailability.CACHED:
                log.warning("Feature Flag evaluation attempted before client has initialized - using last known values from feature store for feature key: " + key)
            else:
                log.warning("Feature Flag evaluation attempted before client has initialized! Feature store unavailable - returning default: " + str(default) + " for feature key: " + key)
                reason = error_reason('CLIENT_NOT_READY')
                self._send_event(event_factory.new_unknown_flag_event(key, context, default, reason))
                return EvaluationDetail(default, None, reason), None

        if not context.valid:
            log.warning("Context was invalid for flag evaluation (%s); returning default value" % context.error)
            return EvaluationDetail(default, None, error_reason('USER_NOT_SPECIFIED')), None

        try:
            flag = await _get_store_item(self._data_system.store, FEATURES, key)
        except Exception as e:
            log.error("Unexpected error while retrieving feature flag \"%s\": %s" % (key, repr(e)))
            log.debug(traceback.format_exc())
            reason = error_reason('EXCEPTION')
            self._send_event(event_factory.new_unknown_flag_event(key, context, default, reason))
            return EvaluationDetail(default, None, reason), None
        if not flag:
            reason = error_reason('FLAG_NOT_FOUND')
            self._send_event(event_factory.new_unknown_flag_event(key, context, default, reason))
            return EvaluationDetail(default, None, reason), None
        else:
            try:
                result = await self._evaluator.evaluate(flag, context, event_factory)
                for event in result.events or []:
                    self._send_event(event)
                detail = result.detail
                if detail.is_default_value():
                    detail = EvaluationDetail(default, None, detail.reason)
                self._send_event(event_factory.new_eval_event(flag, context, detail, default))
                return detail, flag
            except Exception as e:
                log.error("Unexpected error while evaluating feature flag \"%s\": %s" % (key, repr(e)))
                log.debug(traceback.format_exc())
                reason = error_reason('EXCEPTION')
                self._send_event(event_factory.new_default_event(flag, context, default, reason))
                return EvaluationDetail(default, None, reason), flag

    async def all_flags_state(self, context: Context, **kwargs) -> FeatureFlagsState:
        """Returns an object that encapsulates the state of all feature flags for a given context,
        including the flag values and also metadata that can be used on the front end. See the
        JavaScript SDK Reference Guide on
        `Bootstrapping <https://docs.launchdarkly.com/sdk/features/bootstrapping#javascript>`_.

        This method does not send analytics events back to LaunchDarkly.

        :param context: the end context requesting the feature flags
        :param kwargs: optional parameters affecting how the state is computed - see below

        :Keyword Arguments:
          * **client_side_only** (*boolean*) --
            set to True to limit it to only flags that are marked for use with the client-side SDK
            (by default, all flags are included)
          * **with_reasons** (*boolean*) --
            set to True to include evaluation reasons in the state (see :func:`variation_detail()`)
          * **details_only_for_tracked_flags** (*boolean*) --
            set to True to omit any metadata that is normally only used for event generation, such
            as flag versions and evaluation reasons, unless the flag has event tracking or debugging
            turned on

        :return: a FeatureFlagsState object (will never be None; its ``valid`` property will be False
          if the client is offline, has not been initialized, or the context is invalid)
        """
        if self._config.offline:
            log.warning("all_flags_state() called, but client is in offline mode. Returning empty state")
            return FeatureFlagsState(False)

        if self._data_system.data_availability != DataAvailability.REFRESHED:
            if self._data_system.data_availability == DataAvailability.CACHED:
                log.warning("all_flags_state() called before client has finished initializing! Using last known values from feature store")
            else:
                log.warning("all_flags_state() called before client has finished initializing! Feature store unavailable - returning empty state")
                return FeatureFlagsState(False)

        if not context.valid:
            log.warning("Context was invalid for all_flags_state (%s); returning default value" % context.error)
            return FeatureFlagsState(False)

        state = FeatureFlagsState(True)
        client_only = kwargs.get('client_side_only', False)
        with_reasons = kwargs.get('with_reasons', False)
        details_only_if_tracked = kwargs.get('details_only_for_tracked_flags', False)
        try:
            flags_map = await self._data_system.store.all(FEATURES)
            if flags_map is None:
                raise ValueError("feature store error")
        except Exception as e:
            log.error("Unable to read flags for all_flag_state: %s" % repr(e))
            return FeatureFlagsState(False)

        for key, flag in flags_map.items():
            if client_only and not flag.get('clientSide', False):
                continue
            try:
                result = await self._evaluator.evaluate(flag, context, self._event_factory_default)
                detail = result.detail
            except Exception as e:
                log.error("Error evaluating flag \"%s\" in all_flags_state: %s" % (key, repr(e)))
                log.debug(traceback.format_exc())
                reason = {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
                detail = EvaluationDetail(None, None, reason)

            requires_experiment_data = EventFactory.is_experiment(flag, detail.reason)
            flag_state = {
                'key': flag['key'],
                'value': detail.value,
                'variation': detail.variation_index,
                'reason': detail.reason,
                'version': flag['version'],
                'prerequisites': result.prerequisites,
                'trackEvents': flag.get('trackEvents', False) or requires_experiment_data,
                'trackReason': requires_experiment_data,
                'debugEventsUntilDate': flag.get('debugEventsUntilDate', None),
            }

            state.add_flag(flag_state, with_reasons, details_only_if_tracked)

        return state

    def secure_mode_hash(self, context: Context) -> str:
        """Creates a hash string that can be used by the JavaScript SDK to identify a context.

        For more information, see the documentation on
        `Secure mode <https://docs.launchdarkly.com/sdk/features/secure-mode#configuring-secure-mode-in-the-javascript-client-side-sdk>`_.

        :param context: the evaluation context
        :return: the hash string
        """
        return _secure_mode_hash(self._config, context)

    def add_hook(self, hook: AsyncHook):
        """
        Add a hook to the client. In order to register a hook before the client starts, please use the `hooks` property of
        `AsyncConfig`.

        Hooks provide entrypoints which allow for observation of SDK functions.

        The async client only accepts :class:`ldclient.hook.AsyncHook` instances;
        passing a synchronous :class:`ldclient.hook.Hook` raises ``TypeError``.

        :param hook:
        """
        if not isinstance(hook, AsyncHook):
            raise TypeError("AsyncLDClient requires an AsyncHook; synchronous Hook instances are not supported")

        with self.__hooks_lock.write():
            self.__hooks.append(hook)

    async def __evaluate_with_hooks(self, key: str, context: Context, default_value: Any, method: str, block: Callable[[], Any]) -> _EvaluationWithHookResult:
        """
        # evaluate_with_hook will run the provided block, wrapping it with evaluation hook support.
        #
        # :param key:
        # :param context:
        # :param default:
        # :param method:
        # :param block:
        # :return:
        """
        hooks = []  # type: List[AsyncHook]
        with self.__hooks_lock.read():
            if len(self.__hooks) == 0:
                return await block()

            hooks = self.__hooks.copy()

        series_context = EvaluationSeriesContext(key=key, context=context, default_value=default_value, method=method)
        hook_data = await self.__execute_before_evaluation(hooks, series_context)
        evaluation_result = await block()
        await self.__execute_after_evaluation(hooks, series_context, hook_data, evaluation_result.evaluation_detail)

        return evaluation_result

    async def __execute_before_evaluation(self, hooks: List[AsyncHook], series_context: EvaluationSeriesContext) -> List[dict]:
        return [await self.__try_execute_stage("beforeEvaluation", hook.metadata.name, lambda: hook.before_evaluation(series_context, {})) for hook in hooks]

    async def __execute_after_evaluation(self, hooks: List[AsyncHook], series_context: EvaluationSeriesContext, hook_data: List[dict], evaluation_detail: EvaluationDetail) -> List[dict]:
        return [
            await self.__try_execute_stage("afterEvaluation", hook.metadata.name, lambda: hook.after_evaluation(series_context, data, evaluation_detail))
            for (hook, data) in reversed(list(zip(hooks, hook_data)))
        ]

    async def __try_execute_stage(self, method: str, hook_name: str, block: Callable[[], Any]) -> dict:
        try:
            return await block()
        except BaseException as e:
            log.error(f"An error occurred in {method} of the hook {hook_name}: #{e}")
            return {}

    @property
    def big_segment_store_status_provider(self) -> BigSegmentStoreStatusProvider:
        """
        Returns an interface for tracking the status of a Big Segment store.

        The :class:`ldclient.interfaces.BigSegmentStoreStatusProvider` has methods for checking
        whether the Big Segment store is (as far as the SDK knows) currently operational and
        tracking changes in this status.
        """
        return self.__big_segment_store_manager.status_provider

    @property
    def data_source_status_provider(self) -> DataSourceStatusProvider:
        """
        Returns an interface for tracking the status of the data source.

        The data source is the mechanism that the SDK uses to get feature flag configurations, such
        as a streaming connection (the default) or poll requests. The
        :class:`ldclient.interfaces.DataSourceStatusProvider` has methods for checking whether the
        data source is (as far as the SDK knows) currently operational and tracking changes in this
        status.

        :return: The data source status provider
        """
        return self.__data_source_status_provider

    @property
    def data_store_status_provider(self) -> DataStoreStatusProvider:
        """
        Returns an interface for tracking the status of a persistent data store.

        The provider has methods for checking whether the data store is (as far
        as the SDK knows) currently operational, tracking changes in this
        status, and getting cache statistics. These are only relevant for a
        persistent data store; if you are using an in-memory data store, then
        this method will return a stub object that provides no information.

        :return: The data store status provider
        """
        return self.__data_store_status_provider

    @property
    def flag_tracker(self) -> FlagTracker:
        """
        Returns an interface for tracking changes in feature flag configurations.

        The :class:`ldclient.interfaces.FlagTracker` contains methods for
        requesting notifications about feature flag changes using an event
        listener model.
        """
        if not self._started:
            raise RuntimeError("AsyncLDClient.flag_tracker is not available until after start()")
        return self.__flag_tracker

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.close()


__all__ = ['AsyncLDClient']
