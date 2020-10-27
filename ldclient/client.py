"""
This submodule contains the client class that provides most of the SDK functionality.
"""

from typing import Optional, Any, Dict, Mapping
from .impl import AnyNum

import hashlib
import hmac
import threading
import traceback

from ldclient.config import Config, HTTPConfig
from ldclient.diagnostics import create_diagnostic_id, _DiagnosticAccumulator
from ldclient.event_processor import DefaultEventProcessor
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.feature_store import _FeatureStoreDataSetSorter
from ldclient.flag import EvaluationDetail, evaluate, error_reason
from ldclient.flags_state import FeatureFlagsState
from ldclient.impl.event_factory import _EventFactory
from ldclient.impl.stubs import NullEventProcessor, NullUpdateProcessor
from ldclient.interfaces import FeatureStore
from ldclient.polling import PollingUpdateProcessor
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.util import check_uwsgi, log
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind
from ldclient.feature_store import FeatureStore
import queue

from threading import Lock


class _FeatureStoreClientWrapper(FeatureStore):
    """Provides additional behavior that the client requires before or after feature store operations.
    Currently this just means sorting the data set for init(). In the future we may also use this
    to provide an update listener capability.
    """

    def __init__(self, store: FeatureStore):
        self.store = store

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, Dict[Any, Any]]]):
        return self.store.init(_FeatureStoreDataSetSorter.sort_all_collections(all_data))

    def get(self, kind, key, callback):
        return self.store.get(kind, key, callback)

    def all(self, kind, callback):
        return self.store.all(kind, callback)

    def delete(self, kind, key, version):
        return self.store.delete(kind, key, version)

    def upsert(self, kind, item):
        return self.store.upsert(kind, item)

    @property
    def initialized(self) -> bool:
        return self.store.initialized


class LDClient:
    """The LaunchDarkly SDK client object.

    Applications should configure the client at startup time and continue to use it throughout the lifetime
    of the application, rather than creating instances on the fly. The best way to do this is with the
    singleton methods :func:`ldclient.set_config()` and :func:`ldclient.get()`. However, you may also call
    the constructor directly if you need to maintain multiple instances.

    Client instances are thread-safe.
    """
    def __init__(self, config: Config, start_wait: float=5):
        """Constructs a new LDClient instance.

        :param config: optional custom configuration
        :param start_wait: the number of seconds to wait for a successful connection to LaunchDarkly
        """
        check_uwsgi()

        self._config = config
        self._config._validate()

        self._event_processor = None
        self._lock = Lock()
        self._event_factory_default = _EventFactory(False)
        self._event_factory_with_reasons = _EventFactory(True)

        self._store = _FeatureStoreClientWrapper(self._config.feature_store)
        """ :type: FeatureStore """

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")

        if self._config.use_ldd:
            log.info("Started LaunchDarkly Client in LDD mode")

        diagnostic_accumulator = self._set_event_processor(self._config)

        update_processor_ready = threading.Event()
        self._update_processor = self._make_update_processor(self._config, self._store, update_processor_ready, diagnostic_accumulator)
        self._update_processor.start()

        if start_wait > 0 and not self._config.offline and not self._config.use_ldd:
            log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
            update_processor_ready.wait(start_wait)

        if self._update_processor.initialized() is True:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.warning("Initialization timeout exceeded for LaunchDarkly Client or an error occurred. "
                     "Feature Flags may not yet be available.")

    def _set_event_processor(self, config):
        if config.offline or not config.send_events:
            self._event_processor = NullEventProcessor()
            return None
        if not config.event_processor_class:
            diagnostic_id = create_diagnostic_id(config)
            diagnostic_accumulator = None if config.diagnostic_opt_out else _DiagnosticAccumulator(diagnostic_id)
            self._event_processor = DefaultEventProcessor(config, diagnostic_accumulator = diagnostic_accumulator)
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
            feature_requester = FeatureRequesterImpl(config)
        """ :type: FeatureRequester """

        return PollingUpdateProcessor(config, feature_requester, store, ready)

    def get_sdk_key(self) -> Optional[str]:
        """Returns the configured SDK key.
        """
        return self._config.sdk_key

    def close(self):
        """Releases all threads and network connections used by the LaunchDarkly client.

        Do not attempt to use the client after calling this method.
        """
        log.info("Closing LaunchDarkly client..")
        self._event_processor.stop()
        self._update_processor.stop()

    # These magic methods allow a client object to be automatically cleaned up by the "with" scope operator
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _send_event(self, event):
        self._event_processor.send_event(event)

    def track(self, event_name: str, user: dict, data: Optional[Any]=None, metric_value: Optional[AnyNum]=None):
        """Tracks that a user performed an event.

        LaunchDarkly automatically tracks pageviews and clicks that are specified in the Goals
        section of the dashboard. This can be used to track custom goals or other events that do
        not currently have goals.

        :param event_name: the name of the event, which may correspond to a goal in A/B tests
        :param user: the attributes of the user
        :param data: optional additional data associated with the event
        :param metric_value: a numeric value used by the LaunchDarkly experimentation feature in
          numeric custom metrics. Can be omitted if this event is used by only non-numeric metrics.
          This field will also be returned as part of the custom event for Data Export.
        """
        if user is None or user.get('key') is None:
            log.warning("Missing user or user key when calling track().")
        else:
            self._send_event(self._event_factory_default.new_custom_event(event_name, user, data, metric_value))

    def identify(self, user: dict):
        """Registers the user.

        This simply creates an analytics event that will transmit the given user properties to
        LaunchDarkly, so that the user will be visible on your dashboard even if you have not
        evaluated any flags for that user. It has no other effect.

        :param user: attributes of the user to register
        """
        if user is None or user.get('key') is None:
            log.warning("Missing user or user key when calling identify().")
        else:
            self._send_event(self._event_factory_default.new_identify_event(user))

    def is_offline(self) -> bool:
        """Returns true if the client is in offline mode.
        """
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

    def variation(self, key: str, user: dict, default: Any) -> Any:
        """Determines the variation of a feature flag for a user.

        :param key: the unique key for the feature flag
        :param user: a dictionary containing parameters for the end user requesting the flag
        :param default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: one of the flag's variation values, or the default value
        """
        return self._evaluate_internal(key, user, default, self._event_factory_default).value

    def variation_detail(self, key: str, user: dict, default: Any) -> EvaluationDetail:
        """Determines the variation of a feature flag for a user, like :func:`variation()`, but also
        provides additional information about how this value was calculated, in the form of an
        :class:`ldclient.flag.EvaluationDetail` object.

        Calling this method also causes the "reason" data to be included in analytics events,
        if you are capturing detailed event data for this flag.

        :param key: the unique key for the feature flag
        :param user: a dictionary containing parameters for the end user requesting the flag
        :param default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: an object describing the result
        """
        return self._evaluate_internal(key, user, default, self._event_factory_with_reasons)

    def _evaluate_internal(self, key, user, default, event_factory):
        default = self._config.get_default(key, default)

        if self._config.offline:
            return EvaluationDetail(default, None, error_reason('CLIENT_NOT_READY'))

        if not self.is_initialized():
            if self._store.initialized:
                log.warning("Feature Flag evaluation attempted before client has initialized - using last known values from feature store for feature key: " + key)
            else:
                log.warning("Feature Flag evaluation attempted before client has initialized! Feature store unavailable - returning default: "
                         + str(default) + " for feature key: " + key)
                reason = error_reason('CLIENT_NOT_READY')
                self._send_event(event_factory.new_unknown_flag_event(key, user, default, reason))
                return EvaluationDetail(default, None, reason)

        if user is not None and user.get('key', "") == "":
            log.warning("User key is blank. Flag evaluation will proceed, but the user will not be stored in LaunchDarkly.")

        try:
            flag = self._store.get(FEATURES, key, lambda x: x)
        except Exception as e:
            log.error("Unexpected error while retrieving feature flag \"%s\": %s" % (key, repr(e)))
            log.debug(traceback.format_exc())
            reason = error_reason('EXCEPTION')
            self._send_event(event_factory.new_unknown_flag_event(key, user, default, reason))
            return EvaluationDetail(default, None, reason)
        if not flag:
            reason = error_reason('FLAG_NOT_FOUND')
            self._send_event(event_factory.new_unknown_flag_event(key, user, default, reason))
            return EvaluationDetail(default, None, reason)
        else:
            if user is None or user.get('key') is None:
                reason = error_reason('USER_NOT_SPECIFIED')
                self._send_event(event_factory.new_default_event(flag, user, default, reason))
                return EvaluationDetail(default, None, reason)

            try:
                result = evaluate(flag, user, self._store, event_factory)
                for event in result.events or []:
                    self._send_event(event)
                detail = result.detail
                if detail.is_default_value():
                    detail = EvaluationDetail(default, None, detail.reason)
                self._send_event(event_factory.new_eval_event(flag, user, detail, default))
                return detail
            except Exception as e:
                log.error("Unexpected error while evaluating feature flag \"%s\": %s" % (key, repr(e)))
                log.debug(traceback.format_exc())
                reason = error_reason('EXCEPTION')
                self._send_event(event_factory.new_default_event(flag, user, default, reason))
                return EvaluationDetail(default, None, reason)

    def all_flags_state(self, user: dict, **kwargs) -> FeatureFlagsState:
        """Returns an object that encapsulates the state of all feature flags for a given user,
        including the flag values and also metadata that can be used on the front end. See the
        JavaScript SDK Reference Guide on
        `Bootstrapping <https://docs.launchdarkly.com/docs/js-sdk-reference#section-bootstrapping>`_.

        This method does not send analytics events back to LaunchDarkly.

        :param user: the end user requesting the feature flags
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
          if the client is offline, has not been initialized, or the user is None or has no key)
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

        if user is None or user.get('key') is None:
            log.warning("User or user key is None when calling all_flags_state(). Returning empty state.")
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
                detail = evaluate(flag, user, self._store, self._event_factory_default).detail
                state.add_flag(flag, detail.value, detail.variation_index,
                    detail.reason if with_reasons else None, details_only_if_tracked)
            except Exception as e:
                log.error("Error evaluating flag \"%s\" in all_flags_state: %s" % (key, repr(e)))
                log.debug(traceback.format_exc())
                reason = {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
                state.add_flag(flag, None, None, reason if with_reasons else None, details_only_if_tracked)

        return state

    def secure_mode_hash(self, user: dict) -> str:
        """Computes an HMAC signature of a user signed with the client's SDK key,
        for use with the JavaScript SDK.

        For more information, see the JavaScript SDK Reference Guide on
        `Secure mode <https://github.com/launchdarkly/js-client#secure-mode>`_.
        
        :param user: the attributes of the user
        :return: a hash string that can be passed to the front end
        """
        key = user.get('key')
        if key is None or self._config.sdk_key is None:
            return ""
        return hmac.new(self._config.sdk_key.encode(), key.encode(), hashlib.sha256).hexdigest()


__all__ = ['LDClient', 'Config']
