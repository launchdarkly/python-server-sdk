"""
Async client for the LaunchDarkly Server-Side Python SDK.
"""

import asyncio
import traceback
from typing import Any, Callable, List, Optional, Tuple
from uuid import uuid4

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
from ldclient.impl.aio.transport import make_client_session
from ldclient.impl.async_big_segments import AsyncBigSegmentStoreManager
from ldclient.impl.async_evaluator import AsyncEvaluator
from ldclient.impl.async_flag_tracker import AsyncFlagTrackerImpl
from ldclient.impl.client_common import (
    get_environment_metadata,
    get_plugin_hooks
)
from ldclient.impl.client_common import secure_mode_hash as _secure_mode_hash
from ldclient.impl.datasystem import AsyncDataSystem, DataAvailability
from ldclient.impl.evaluator_common import error_reason
from ldclient.impl.events.async_event_processor import (
    DefaultAsyncEventProcessor
)
from ldclient.impl.events.diagnostics import (
    _DiagnosticAccumulator,
    create_diagnostic_id
)
from ldclient.impl.events.types import EventFactory
from ldclient.impl.model.feature_flag import FeatureFlag
from ldclient.impl.stubs import AsyncNullEventProcessor
from ldclient.impl.util import log
from ldclient.interfaces import (
    AsyncBigSegmentStoreStatusProvider,
    AsyncFeatureStore,
    AsyncFlagTracker,
    DataSourceStatusProvider,
    DataStoreStatusProvider
)
from ldclient.migrations import OpTracker, Stage
from ldclient.plugin import EnvironmentMetadata
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


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
        # Event processor is a no-op until start(); track/identify before start()
        # drop events.
        self._event_processor: Any = AsyncNullEventProcessor()

        self.__hooks: List = list(config.hooks)

        self._event_factory_default = EventFactory(False)
        self._event_factory_with_reasons = EventFactory(True)

        # Build the object graph here (loop-free). start() supplies the loop-bound
        # resources: the HTTP session (created lazily), the data source, the
        # big-segment poll, and the event processor. Evaluation before start()
        # serves whatever the store already has.
        self._data_system: AsyncDataSystem = self._make_data_system()

        self.__data_store_status_provider = self._data_system.data_store_status_provider
        self.__data_source_status_provider = self._data_system.data_source_status_provider

        self.__big_segment_store_manager = AsyncBigSegmentStoreManager(self._config.big_segments)

        async def get_flag_fn(key):
            return await self._data_system.store.get(FEATURES, key)

        async def get_segment_fn(key):
            return await self._data_system.store.get(SEGMENTS, key)

        async def get_membership_fn(key):
            return await self.__big_segment_store_manager.get_user_membership(key)

        self._evaluator = AsyncEvaluator(
            get_flag_fn,
            get_segment_fn,
            get_membership_fn,
            log,
        )

        async def variation_eval_fn(key, context):
            return await self.variation(key, context, None)

        self.__flag_tracker = AsyncFlagTrackerImpl(
            self._data_system.flag_change_listeners,
            variation_eval_fn
        )

    async def start(self, start_wait: float = 5.0) -> None:
        """Start the client's background work: create the shared HTTP session and
        start the data source, the big-segment status poll, and the event processor.

        Single-shot. Calling start() again after it has started -- or after a
        failed start -- is a no-op; construct a new client to retry. Calling
        start() after close() is also a logged no-op.

        :param start_wait: seconds to wait for the data source to initialize
        """
        async with self._lifecycle_lock:
            if self._closed:
                log.warning("start() called on a closed AsyncLDClient; ignoring")
                return
            if self._started:
                return

            self._started = True

            try:
                await self.__start_up(start_wait)
            except BaseException:
                # Catch BaseException, not Exception, so a cancelled start
                # (CancelledError) also tears down what __start_up began.
                self._closed = True
                await self._close_components()
                raise

    async def close(self) -> None:
        """Shut down the client and release all resources.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True

            # A client that was never started has nothing running to release.
            if self._started:
                await self._close_components()

    async def _close_components(self):
        """Stop the SDK components and release the shared HTTP session."""
        log.info("Closing LaunchDarkly client..")

        await self._data_system.stop()
        await self.__big_segment_store_manager.stop()

        # The event processor is last because it may still be sending events the
        # other components generated.
        await self._event_processor.stop()

        if self._session is not None:
            try:
                await self._session.close()
            except Exception as e:
                log.warning("Error closing HTTP session: %s", e)
            self._session = None

    async def __start_up(self, start_wait: float):
        environment_metadata = get_environment_metadata(self._config, "python-server-sdk-async")
        plugin_hooks = get_plugin_hooks(self._config.plugins, environment_metadata)

        # Append plugin hooks to those already registered (the config hooks plus
        # any added via add_hook() before start()), rather than overwriting.
        self.__hooks = self.__hooks + plugin_hooks

        # Start the big-segment status poll now that a loop is running.
        self.__big_segment_store_manager.start()

        # FDv2 builds its data sources from builders; wire the shared session into
        # them before starting (FDv1 pulls the session itself via its provider).
        datasystem_config = self._config.datasystem_config
        if datasystem_config is not None and not self._config.offline:
            self._wire_data_source_sessions(datasystem_config)

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

        if await self.is_initialized() is True:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.warning("Initialization timeout exceeded for LaunchDarkly Client or an error occurred. " "Feature Flags may not yet be available.")

    def _get_session(self):
        """Return the shared aiohttp session, creating it on first use inside the
        event loop. Nothing creates it in offline/LDD mode, because no network
        component asks for it. Uses the same factory as the async transport so
        SSL/cert setup and proxy handling stay consistent (proxies are resolved
        per request, so the session itself has trust_env=False)."""
        if self._session is None:
            self._session = make_client_session(self._config)
        return self._session

    def _make_data_system(self) -> AsyncDataSystem:
        datasystem_config = self._config.datasystem_config
        if datasystem_config is None:
            from ldclient.impl.datasystem.async_fdv1 import AsyncFDv1

            return AsyncFDv1(self._config, self._select_feature_store(), self._get_session)

        from ldclient.impl.datasystem.async_fdv2 import AsyncFDv2

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
                builder.session(self._get_session())

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
            self._event_processor = DefaultAsyncEventProcessor(config, self._get_session(), diagnostic_accumulator=diagnostic_accumulator)
            return diagnostic_accumulator
        self._event_processor = config.event_processor_class(config)
        return None

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
            log.error("error generating migration op event %s; no event will be emitted", event)
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

        This method simply creates an analytics event containing the context properties, so
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

    async def is_initialized(self) -> bool:
        """Returns true if the client has successfully connected to LaunchDarkly.

        If this returns false, it means that the client has not yet successfully connected to LaunchDarkly.
        It might still be in the process of starting up, or it might be attempting to reconnect after an
        unsuccessful attempt, or it might have received an unrecoverable error (such as an invalid SDK key)
        and given up.

        This is a coroutine because determining readiness may query a persistent store.
        """
        if self.is_offline() or self._config.use_ldd:
            return True

        return (await self._data_system.data_availability()).at_least(DataAvailability.CACHED)

    async def flush(self):
        """Flushes all pending analytics events.

        Normally, batches of events are delivered in the background at intervals determined by the
        ``flush_interval`` property of :class:`ldclient.config.Config`. Calling ``flush()``
        schedules the next event delivery to be as soon as possible; however, the delivery still
        happens asynchronously in the background, so this method will return immediately.
        """
        if self._config.offline:
            return
        # flush() only schedules delivery; it does not await, so there is
        # nothing to await here.
        self._event_processor.flush()

    async def flush_and_wait(self, timeout: float) -> bool:
        """Flushes all pending analytics events and waits for delivery to complete.

        Unlike :meth:`flush`, this waits for the buffered events to be delivered, up to ``timeout``
        seconds. Returns True if delivery completed within the timeout, or False if it timed out.

        :param timeout: the maximum number of seconds to wait for delivery
        """
        if self._config.offline:
            return True
        return await self._event_processor.flush_and_wait(timeout)

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

        availability = await self._data_system.data_availability()
        if availability != DataAvailability.REFRESHED:
            if availability == DataAvailability.CACHED:
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
            flag = await self._data_system.store.get(FEATURES, key)
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

        availability = await self._data_system.data_availability()
        if availability != DataAvailability.REFRESHED:
            if availability == DataAvailability.CACHED:
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
                prerequisites = result.prerequisites
            except Exception as e:
                log.error("Error evaluating flag \"%s\" in all_flags_state: %s" % (key, repr(e)))
                log.debug(traceback.format_exc())
                reason = {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
                detail = EvaluationDetail(None, None, reason)
                prerequisites = []
            requires_experiment_data = EventFactory.is_experiment(flag, detail.reason)
            flag_state = {
                'key': flag['key'],
                'value': detail.value,
                'variation': detail.variation_index,
                'reason': detail.reason,
                'version': flag['version'],
                'prerequisites': prerequisites,
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
        # Snapshot the hook list to ensure hooks added during evaluation don't get called for the current evaluation.
        hooks = list(self.__hooks)  # type: List[AsyncHook]

        if not hooks:
            return await block()

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
        except asyncio.CancelledError:
            # Do not swallow cancellation; it must propagate for shutdown.
            raise
        except BaseException as e:
            log.error(f"An error occurred in {method} of the hook {hook_name}: #{e}")
            return {}

    @property
    def big_segment_store_status_provider(self) -> AsyncBigSegmentStoreStatusProvider:
        """
        Returns an interface for tracking the status of a Big Segment store.

        The :class:`ldclient.interfaces.AsyncBigSegmentStoreStatusProvider` has methods for
        checking whether the Big Segment store is (as far as the SDK knows) currently
        operational and tracking changes in this status.
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
    def flag_tracker(self) -> AsyncFlagTracker:
        """
        Returns an interface for tracking changes in feature flag configurations.

        The :class:`ldclient.interfaces.AsyncFlagTracker` contains methods for
        requesting notifications about feature flag changes using an event
        listener model.

        Listeners registered before ``start()`` receive change events once the
        data system starts.
        """
        return self.__flag_tracker

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.close()


__all__ = ['AsyncLDClient']
