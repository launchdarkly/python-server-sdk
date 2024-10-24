"""
This submodule contains the client class that provides most of the SDK functionality.
"""

import hashlib
import hmac
import threading
import traceback
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from ldclient.config import Config
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail, FeatureFlagsState
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.hook import (EvaluationSeriesContext, Hook,
                           _EvaluationWithHookResult)
from ldclient.impl.big_segments import BigSegmentStoreManager
from ldclient.impl.datasource.feature_requester import FeatureRequesterImpl
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.datasource.status import (DataSourceStatusProviderImpl,
                                             DataSourceUpdateSinkImpl)
from ldclient.impl.datasource.streaming import StreamingUpdateProcessor
from ldclient.impl.datastore.status import (DataStoreStatusProviderImpl,
                                            DataStoreUpdateSinkImpl)
from ldclient.impl.evaluator import Evaluator, error_reason
from ldclient.impl.events.diagnostics import (_DiagnosticAccumulator,
                                              create_diagnostic_id)
from ldclient.impl.events.event_processor import DefaultEventProcessor
from ldclient.impl.events.types import EventFactory
from ldclient.impl.flag_tracker import FlagTrackerImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.model.feature_flag import FeatureFlag
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.impl.stubs import NullEventProcessor, NullUpdateProcessor
from ldclient.impl.util import check_uwsgi, log
from ldclient.interfaces import (BigSegmentStoreStatusProvider,
                                 DataSourceStatusProvider, DataStoreStatus,
                                 DataStoreStatusProvider, DataStoreUpdateSink,
                                 FeatureStore, FlagTracker)
from ldclient.migrations import OpTracker, Stage
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

from .impl import AnyNum


class _FeatureStoreClientWrapper(FeatureStore):
    """Provides additional behavior that the client requires before or after feature store operations.
    Currently this just means sorting the data set for init() and dealing with data store status listeners.
    """

    def __init__(self, store: FeatureStore, store_update_sink: DataStoreUpdateSink):
        self.store = store
        self.__store_update_sink = store_update_sink
        self.__monitoring_enabled = self.is_monitoring_enabled()

        # Covers the following variables
        self.__lock = ReadWriteLock()
        self.__last_available = True
        self.__poller: Optional[RepeatingTask] = None

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        return self.__wrapper(lambda: self.store.init(_FeatureStoreDataSetSorter.sort_all_collections(all_data)))

    def get(self, kind, key, callback):
        return self.__wrapper(lambda: self.store.get(kind, key, callback))

    def all(self, kind, callback):
        return self.__wrapper(lambda: self.store.all(kind, callback))

    def delete(self, kind, key, version):
        return self.__wrapper(lambda: self.store.delete(kind, key, version))

    def upsert(self, kind, item):
        return self.__wrapper(lambda: self.store.upsert(kind, item))

    @property
    def initialized(self) -> bool:
        return self.store.initialized

    def __wrapper(self, fn: Callable):
        try:
            return fn()
        except BaseException:
            if self.__monitoring_enabled:
                self.__update_availability(False)
            raise

    def __update_availability(self, available: bool):
        try:
            self.__lock.lock()
            if available == self.__last_available:
                return
            self.__last_available = available
        finally:
            self.__lock.unlock()

        status = DataStoreStatus(available, False)

        if available:
            log.warn("Persistent store is available again")

        self.__store_update_sink.update_status(status)

        if available:
            try:
                self.__lock.lock()
                if self.__poller is not None:
                    self.__poller.stop()
                    self.__poller = None
            finally:
                self.__lock.unlock()

            return

        log.warn("Detected persistent store unavailability; updates will be cached until it recovers")
        task = RepeatingTask("ldclient.check-availability", 0.5, 0, self.__check_availability)

        self.__lock.lock()
        self.__poller = task
        self.__poller.start()
        self.__lock.unlock()

    def __check_availability(self):
        try:
            if self.store.is_available():
                self.__update_availability(True)
        except BaseException as e:
            log.error("Unexpected error from data store status function: %s", e)

    def is_monitoring_enabled(self) -> bool:
        """
        This methods determines whether the wrapped store can support enabling monitoring.

        The wrapped store must provide a monitoring_enabled method, which must
        be true. But this alone is not sufficient.

        Because this class wraps all interactions with a provided store, it can
        technically "monitor" any store. However, monitoring also requires that
        we notify listeners when the store is available again.

        We determine this by checking the store's `available?` method, so this
        is also a requirement for monitoring support.

        These extra checks won't be necessary once `available` becomes a part
        of the core interface requirements and this class no longer wraps every
        feature store.
        """

        if not hasattr(self.store, 'is_monitoring_enabled'):
            return False

        if not hasattr(self.store, 'is_available'):
            return False

        monitoring_enabled = getattr(self.store, 'is_monitoring_enabled')
        if not callable(monitoring_enabled):
            return False

        return monitoring_enabled()


def _get_store_item(store, kind: VersionedDataKind, key: str) -> Any:
    # This decorator around store.get provides backward compatibility with any custom data
    # store implementation that might still be returning a dict, instead of our data model
    # classes like FeatureFlag.
    item = store.get(kind, key, lambda x: x)
    return kind.decode(item) if isinstance(item, dict) else item


class LDClient:
    """The LaunchDarkly SDK client object.

    Applications should configure the client at startup time and continue to use it throughout the lifetime
    of the application, rather than creating instances on the fly. The best way to do this is with the
    singleton methods :func:`ldclient.set_config()` and :func:`ldclient.get()`. However, you may also call
    the constructor directly if you need to maintain multiple instances.

    Client instances are thread-safe.
    """

    def __init__(self, config: Config, start_wait: float = 5):
        """Constructs a new LDClient instance.

        :param config: optional custom configuration
        :param start_wait: the number of seconds to wait for a successful connection to LaunchDarkly
        """
        check_uwsgi()

        self._config = config
        self._config._validate()

        self.__hooks_lock = ReadWriteLock()
        self.__hooks = config.hooks  # type: List[Hook]

        self._event_processor = None
        self._event_factory_default = EventFactory(False)
        self._event_factory_with_reasons = EventFactory(True)

        data_store_listeners = Listeners()
        store_sink = DataStoreUpdateSinkImpl(data_store_listeners)
        store = _FeatureStoreClientWrapper(self._config.feature_store, store_sink)

        self.__data_store_status_provider = DataStoreStatusProviderImpl(store, store_sink)

        data_source_listeners = Listeners()
        flag_change_listeners = Listeners()

        self.__flag_tracker = FlagTrackerImpl(flag_change_listeners, lambda key, context: self.variation(key, context, None))

        self._config._data_source_update_sink = DataSourceUpdateSinkImpl(store, data_source_listeners, flag_change_listeners)
        self.__data_source_status_provider = DataSourceStatusProviderImpl(data_source_listeners, self._config._data_source_update_sink)
        self._store = store  # type: FeatureStore

        big_segment_store_manager = BigSegmentStoreManager(self._config.big_segments)
        self.__big_segment_store_manager = big_segment_store_manager

        self._evaluator = Evaluator(
            lambda key: _get_store_item(store, FEATURES, key), lambda key: _get_store_item(store, SEGMENTS, key), lambda key: big_segment_store_manager.get_user_membership(key), log
        )

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")

        if self._config.use_ldd:
            log.info("Started LaunchDarkly Client in LDD mode")

        diagnostic_accumulator = self._set_event_processor(self._config)

        update_processor_ready = threading.Event()
        self._update_processor = self._make_update_processor(self._config, self._store, update_processor_ready, diagnostic_accumulator)
        self._update_processor.start()

        if not self._config.offline and not self._config.use_ldd:
            if start_wait > 60:
                log.warning(f"Client was configured to block for up to {start_wait} seconds when initializing. We recommend blocking no longer than 60.")

            if start_wait > 0:
                log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
                update_processor_ready.wait(start_wait)

        if self._update_processor.initialized() is True:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.warning("Initialization timeout exceeded for LaunchDarkly Client or an error occurred. " "Feature Flags may not yet be available.")

    def _set_event_processor(self, config):
        if config.offline or not config.send_events:
            self._event_processor = NullEventProcessor()
            return None
        if not config.event_processor_class:
            diagnostic_id = create_diagnostic_id(config)
            diagnostic_accumulator = None if config.diagnostic_opt_out else _DiagnosticAccumulator(diagnostic_id)
            self._event_processor = DefaultEventProcessor(config, diagnostic_accumulator=diagnostic_accumulator)
            return diagnostic_accumulator
        self._event_processor = config.event_processor_class(config)
        return None

    def _make_update_processor(self, config, store, ready, diagnostic_accumulator):
        if config.update_processor_class:
            log.info("Using user-specified update processor: " + str(config.update_processor_class))
            return config.update_processor_class(config, store, ready)

        if config.offline or config.use_ldd:
            return NullUpdateProcessor(config, store, ready)

        if config.stream:
            return StreamingUpdateProcessor(config, store, ready, diagnostic_accumulator)

        log.info("Disabling streaming API")
        log.warning("You should only disable the streaming API if instructed to do so by LaunchDarkly support")

        if config.feature_requester_class:
            feature_requester = config.feature_requester_class(config)
        else:
            feature_requester = FeatureRequesterImpl(config)  # type: FeatureRequester

        return PollingUpdateProcessor(config, feature_requester, store, ready)

    def get_sdk_key(self) -> Optional[str]:
        """Returns the configured SDK key."""
        return self._config.sdk_key

    def close(self):
        """Releases all threads and network connections used by the LaunchDarkly client.

        Do not attempt to use the client after calling this method.
        """
        log.info("Closing LaunchDarkly client..")
        self._event_processor.stop()
        self._update_processor.stop()
        self.__big_segment_store_manager.stop()

    # These magic methods allow a client object to be automatically cleaned up by the "with" scope operator
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

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
        return self.is_offline() or self._config.use_ldd or self._update_processor.initialized()

    def flush(self):
        """Flushes all pending analytics events.

        Normally, batches of events are delivered in the background at intervals determined by the
        ``flush_interval`` property of :class:`ldclient.config.Config`. Calling ``flush()``
        schedules the next event delivery to be as soon as possible; however, the delivery still
        happens asynchronously on a worker thread, so this method will return immediately.
        """
        if self._config.offline:
            return
        return self._event_processor.flush()

    def variation(self, key: str, context: Context, default: Any) -> Any:
        """Calculates the value of a feature flag for a given context.

        :param key: the unique key for the feature flag
        :param context: the evaluation context
        :param default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: the variation for the given context, or the ``default`` value if the flag cannot be evaluated
        """

        def evaluate():
            detail, _ = self._evaluate_internal(key, context, default, self._event_factory_default)
            return _EvaluationWithHookResult(evaluation_detail=detail)

        return self.__evaluate_with_hooks(key=key, context=context, default_value=default, method="variation", block=evaluate).evaluation_detail.value

    def variation_detail(self, key: str, context: Context, default: Any) -> EvaluationDetail:
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

        def evaluate():
            detail, _ = self._evaluate_internal(key, context, default, self._event_factory_with_reasons)
            return _EvaluationWithHookResult(evaluation_detail=detail)

        return self.__evaluate_with_hooks(key=key, context=context, default_value=default, method="variation_detail", block=evaluate).evaluation_detail

    def migration_variation(self, key: str, context: Context, default_stage: Stage) -> Tuple[Stage, OpTracker]:
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

        def evaluate():
            detail, flag = self._evaluate_internal(key, context, default_stage.value, self._event_factory_default)

            if isinstance(detail.value, str):
                stage = Stage.from_str(detail.value)
                if stage is not None:
                    tracker = OpTracker(key, flag, context, detail, default_stage)
                    return _EvaluationWithHookResult(evaluation_detail=detail, results={'default_stage': stage, 'tracker': tracker})

            detail = EvaluationDetail(default_stage.value, None, error_reason('WRONG_TYPE'))
            tracker = OpTracker(key, flag, context, detail, default_stage)
            return _EvaluationWithHookResult(evaluation_detail=detail, results={'default_stage': default_stage, 'tracker': tracker})

        hook_result = self.__evaluate_with_hooks(key=key, context=context, default_value=default_stage.value, method="migration_variation", block=evaluate)
        return hook_result.results['default_stage'], hook_result.results['tracker']

    def _evaluate_internal(self, key: str, context: Context, default: Any, event_factory) -> Tuple[EvaluationDetail, Optional[FeatureFlag]]:
        default = self._config.get_default(key, default)

        if self._config.offline:
            return EvaluationDetail(default, None, error_reason('CLIENT_NOT_READY')), None

        if not self.is_initialized():
            if self._store.initialized:
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
            flag = _get_store_item(self._store, FEATURES, key)
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
                result = self._evaluator.evaluate(flag, context, event_factory)
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

    def all_flags_state(self, context: Context, **kwargs) -> FeatureFlagsState:
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

        if not self.is_initialized():
            if self._store.initialized:
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
            flags_map = self._store.all(FEATURES, lambda x: x)
            if flags_map is None:
                raise ValueError("feature store error")
        except Exception as e:
            log.error("Unable to read flags for all_flag_state: %s" % repr(e))
            return FeatureFlagsState(False)

        for key, flag in flags_map.items():
            if client_only and not flag.get('clientSide', False):
                continue
            try:
                result = self._evaluator.evaluate(flag, context, self._event_factory_default)
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
        if not context.valid:
            log.warning("Context was invalid for secure_mode_hash (%s); returning empty hash" % context.error)
            return ""
        return hmac.new(str(self._config.sdk_key).encode(), context.fully_qualified_key.encode(), hashlib.sha256).hexdigest()

    def add_hook(self, hook: Hook):
        """
        Add a hook to the client. In order to register a hook before the client starts, please use the `hooks` property of
        `Config`.

        Hooks provide entrypoints which allow for observation of SDK functions.

        :param hook:
        """
        if not isinstance(hook, Hook):
            return

        self.__hooks_lock.lock()
        self.__hooks.append(hook)
        self.__hooks_lock.unlock()

    def __evaluate_with_hooks(self, key: str, context: Context, default_value: Any, method: str, block: Callable[[], _EvaluationWithHookResult]) -> _EvaluationWithHookResult:
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
        hooks = []  # type: List[Hook]
        try:
            self.__hooks_lock.rlock()

            if len(self.__hooks) == 0:
                return block()

            hooks = self.__hooks.copy()
        finally:
            self.__hooks_lock.runlock()

        series_context = EvaluationSeriesContext(key=key, context=context, default_value=default_value, method=method)
        hook_data = self.__execute_before_evaluation(hooks, series_context)
        evaluation_result = block()
        self.__execute_after_evaluation(hooks, series_context, hook_data, evaluation_result.evaluation_detail)

        return evaluation_result

    def __execute_before_evaluation(self, hooks: List[Hook], series_context: EvaluationSeriesContext) -> List[dict]:
        return [self.__try_execute_stage("beforeEvaluation", hook.metadata.name, lambda: hook.before_evaluation(series_context, {})) for hook in hooks]

    def __execute_after_evaluation(self, hooks: List[Hook], series_context: EvaluationSeriesContext, hook_data: List[dict], evaluation_detail: EvaluationDetail) -> List[dict]:
        return [
            self.__try_execute_stage("afterEvaluation", hook.metadata.name, lambda: hook.after_evaluation(series_context, data, evaluation_detail))
            for (hook, data) in reversed(list(zip(hooks, hook_data)))
        ]

    def __try_execute_stage(self, method: str, hook_name: str, block: Callable[[], dict]) -> dict:
        try:
            return block()
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
        return self.__flag_tracker


__all__ = ['LDClient', 'Config']
