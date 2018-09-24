from __future__ import division, with_statement, absolute_import

import hashlib
import hmac
import threading
import traceback

from builtins import object

from ldclient.config import Config as Config
from ldclient.event_processor import NullEventProcessor
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.flag import EvaluationDetail, evaluate, error_reason
from ldclient.flags_state import FeatureFlagsState
from ldclient.polling import PollingUpdateProcessor
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.util import check_uwsgi, log
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue  # Python 3

from threading import Lock


class LDClient(object):
    def __init__(self, sdk_key=None, config=None, start_wait=5):
        """Constructs a new LDClient instance.

        Rather than calling this constructor directly, you can call the `ldclient.set_sdk_key`,
        `ldclient.set_config`, and `ldclient.get` functions to configure and use a singleton
        client instance.

        :param string sdk_key: the SDK key for your LaunchDarkly environment
        :param Config config: optional custom configuration
        :param float start_wait: the number of seconds to wait for a successful connection to LaunchDarkly
        """
        check_uwsgi()

        if config is not None and config.sdk_key is not None and sdk_key is not None:
            raise Exception("LaunchDarkly client init received both sdk_key and config with sdk_key. "
                            "Only one of either is expected")

        if sdk_key is not None:
            log.warn("Deprecated sdk_key argument was passed to init. Use config object instead.")
            self._config = Config(sdk_key=sdk_key)
        else:
            self._config = config or Config.default()
        self._config._validate()

        self._event_processor = None
        self._lock = Lock()

        self._store = self._config.feature_store
        """ :type: FeatureStore """

        if self._config.offline or not self._config.send_events:
            self._event_processor = NullEventProcessor()
        else:
            self._event_processor = self._config.event_processor_class(self._config)

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")
            return

        if self._config.use_ldd:
            log.info("Started LaunchDarkly Client in LDD mode")
            return

        update_processor_ready = threading.Event()

        if self._config.update_processor_class:
            log.info("Using user-specified update processor: " + str(self._config.update_processor_class))
            self._update_processor = self._config.update_processor_class(
                self._config, self._store, update_processor_ready)
        else:
            if self._config.feature_requester_class:
                feature_requester = self._config.feature_requester_class(self._config)
            else:
                feature_requester = FeatureRequesterImpl(self._config)
            """ :type: FeatureRequester """

            if self._config.stream:
                self._update_processor = StreamingUpdateProcessor(
                    self._config, feature_requester, self._store, update_processor_ready)
            else:
                log.info("Disabling streaming API")
                log.warn("You should only disable the streaming API if instructed to do so by LaunchDarkly support")
                self._update_processor = PollingUpdateProcessor(
                    self._config, feature_requester, self._store, update_processor_ready)
        """ :type: UpdateProcessor """

        self._update_processor.start()
        log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
        update_processor_ready.wait(start_wait)

        if self._update_processor.initialized() is True:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.warn("Initialization timeout exceeded for LaunchDarkly Client or an error occurred. "
                     "Feature Flags may not yet be available.")

    def get_sdk_key(self):
        """Returns the configured SDK key.

        :rtype: string
        """
        return self._config.sdk_key

    def close(self):
        """Releases all threads and network connections used by the LaunchDarkly client.
        
        Do not attempt to use the client after calling this method.
        """
        log.info("Closing LaunchDarkly client..")
        if self.is_offline():
            return
        if self._event_processor:
            self._event_processor.stop()
        if self._update_processor and self._update_processor.is_alive():
            self._update_processor.stop()

    def _send_event(self, event):
        self._event_processor.send_event(event)

    def track(self, event_name, user, data=None):
        """Tracks that a user performed an event.

        :param string event_name: The name of the event.
        :param dict user: The attributes of the user.
        :param data: Optional additional data associated with the event.
        """
        self._sanitize_user(user)
        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when calling track().")
        self._send_event({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def identify(self, user):
        """Registers the user.

        :param dict user: attributes of the user to register
        """
        self._sanitize_user(user)
        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when calling identify().")
        self._send_event({'kind': 'identify', 'key': user.get('key'), 'user': user})

    def is_offline(self):
        """Returns true if the client is in offline mode.

        :rtype: bool
        """
        return self._config.offline

    def is_initialized(self):
        """Returns true if the client has successfully connected to LaunchDarkly.

        :rype: bool
        """
        return self.is_offline() or self._config.use_ldd or self._update_processor.initialized()

    def flush(self):
        """Flushes all pending events.
        """
        if self._config.offline:
            return
        return self._event_processor.flush()

    def toggle(self, key, user, default):
        """Deprecated synonym for `variation`.
        """
        log.warn("Deprecated method: toggle() called. Use variation() instead.")
        return self.variation(key, user, default)

    def variation(self, key, user, default):
        """Determines the variation of a feature flag for a user.

        :param string key: the unique key for the feature flag
        :param dict user: a dictionary containing parameters for the end user requesting the flag
        :param object default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: one of the flag's variation values, or the default value
        """
        return self._evaluate_internal(key, user, default, False).value
    
    def variation_detail(self, key, user, default):
        """Determines the variation of a feature flag for a user, like `variation`, but also
        provides additional information about how this value was calculated.
        
        The return value is an EvaluationDetail object, which has three properties:

        `value`: the value that was calculated for this user (same as the return value
        of `variation`)
        
        `variation_index`: the positional index of this value in the flag, e.g. 0 for the
        first variation - or `None` if the default value was returned
        
        `reason`: a hash describing the main reason why this value was selected.
        
        The `reason` will also be included in analytics events, if you are capturing
        detailed event data for this flag.
        
        :param string key: the unique key for the feature flag
        :param dict user: a dictionary containing parameters for the end user requesting the flag
        :param object default: the default value of the flag, to be used if the value is not
          available from LaunchDarkly
        :return: an EvaluationDetail object describing the result
        :rtype: EvaluationDetail
        """
        return self._evaluate_internal(key, user, default, True)
    
    def _evaluate_internal(self, key, user, default, include_reasons_in_events):
        default = self._config.get_default(key, default)

        if self._config.offline:
            return EvaluationDetail(default, None, error_reason('CLIENT_NOT_READY'))
        
        if user is not None:
            self._sanitize_user(user)

        def send_event(value, variation=None, flag=None, reason=None):
            self._send_event({'kind': 'feature', 'key': key, 'user': user,
                              'value': value, 'variation': variation, 'default': default,
                              'version': flag.get('version') if flag else None,
                              'trackEvents': flag.get('trackEvents') if flag else None,
                              'debugEventsUntilDate': flag.get('debugEventsUntilDate') if flag else None,
                              'reason': reason if include_reasons_in_events else None})

        if not self.is_initialized():
            if self._store.initialized:
                log.warn("Feature Flag evaluation attempted before client has initialized - using last known values from feature store for feature key: " + key)
            else:
                log.warn("Feature Flag evaluation attempted before client has initialized! Feature store unavailable - returning default: "
                         + str(default) + " for feature key: " + key)
                reason = error_reason('CLIENT_NOT_READY')
                send_event(default, None, None, reason)
                return EvaluationDetail(default, None, reason)
        
        if user is not None and user.get('key', "") == "":
            log.warn("User key is blank. Flag evaluation will proceed, but the user will not be stored in LaunchDarkly.")

        flag = self._store.get(FEATURES, key, lambda x: x)
        if not flag:
            reason = error_reason('FLAG_NOT_FOUND')
            send_event(default, None, None, reason)
            return EvaluationDetail(default, None, reason)
        else:
            if user is None or user.get('key') is None:
                reason = error_reason('USER_NOT_SPECIFIED')
                send_event(default, None, flag, reason)
                return EvaluationDetail(default, None, reason)

            try:
                result = evaluate(flag, user, self._store, include_reasons_in_events)
                for event in result.events or []:
                    self._send_event(event)
                detail = result.detail
                if detail.is_default_value():
                    detail = EvaluationDetail(default, None, detail.reason)
                send_event(detail.value, detail.variation_index, flag, detail.reason)
                return detail
            except Exception as e:
                log.error("Unexpected error while evaluating feature flag \"%s\": %s" % (key, e))
                log.debug(traceback.format_exc())
                reason = error_reason('EXCEPTION')
                send_event(default, None, flag, reason)
                return EvaluationDetail(default, None, reason)
    
    def all_flags(self, user):
        """Returns all feature flag values for the given user.
        
        This method is deprecated - please use `all_flags_state` instead. Current versions of the
        client-side SDK will not generate analytics events correctly if you pass the result of `all_flags`.

        :param dict user: the end user requesting the feature flags
        :return: a dictionary of feature flag keys to values; returns None if the client is offline,
          has not been initialized, or the user is None or has no key
        :rtype: dict
        """
        state = self.all_flags_state(user)
        if not state.valid:
            return None
        return state.to_values_map()
    
    def all_flags_state(self, user, **kwargs):
        """Returns an object that encapsulates the state of all feature flags for a given user,
        including the flag values and also metadata that can be used on the front end. 
        
        This method does not send analytics events back to LaunchDarkly.

        :param dict user: the end user requesting the feature flags
        :param kwargs: optional parameters affecting how the state is computed: set
          `client_side_only=True` to limit it to only flags that are marked for use with the
          client-side SDK (by default, all flags are included); set `with_reasons=True` to
          include evaluation reasons in the state (see `variation_detail`)
        :return: a FeatureFlagsState object (will never be None; its 'valid' property will be False
          if the client is offline, has not been initialized, or the user is None or has no key)
        :rtype: FeatureFlagsState
        """
        if self._config.offline:
            log.warn("all_flags_state() called, but client is in offline mode. Returning empty state")
            return FeatureFlagsState(False)

        if not self.is_initialized():
            if self._store.initialized:
                log.warn("all_flags_state() called before client has finished initializing! Using last known values from feature store")
            else:
                log.warn("all_flags_state() called before client has finished initializing! Feature store unavailable - returning empty state")
                return FeatureFlagsState(False)

        if user is None or user.get('key') is None:
            log.warn("User or user key is None when calling all_flags_state(). Returning empty state.")
            return FeatureFlagsState(False)
        
        state = FeatureFlagsState(True)
        client_only = kwargs.get('client_side_only', False)
        with_reasons = kwargs.get('with_reasons', False)
        try:
            flags_map = self._store.all(FEATURES, lambda x: x)
            if flags_map is None:
                raise ValueError("feature store error")
        except Exception as e:
            log.error("Unable to read flags for all_flag_state: %s" % e)
            return FeatureFlagsState(False)
        
        for key, flag in flags_map.items():
            if client_only and not flag.get('clientSide', False):
                continue
            try:
                detail = evaluate(flag, user, self._store, False).detail
                state.add_flag(flag, detail.value, detail.variation_index,
                    detail.reason if with_reasons else None)
            except Exception as e:
                log.error("Error evaluating flag \"%s\" in all_flags_state: %s" % (key, e))
                log.debug(traceback.format_exc())
                reason = {'kind': 'ERROR', 'errorKind': 'EXCEPTION'}
                state.add_flag(flag, None, None, reason if with_reasons else None)
        
        return state
    
    def secure_mode_hash(self, user):
        """Generates a hash value for a user.

        For more info: <a href="https://github.com/launchdarkly/js-client#secure-mode">https://github.com/launchdarkly/js-client#secure-mode</a>
        
        :param dict user: the attributes of the user
        :return: a hash string that can be passed to the front end
        :rtype: string
        """
        if user.get('key') is None or self._config.sdk_key is None:
            return ""
        return hmac.new(self._config.sdk_key.encode(), user.get('key').encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def _sanitize_user(user):
        if 'key' in user:
            user['key'] = str(user['key'])


__all__ = ['LDClient', 'Config']
