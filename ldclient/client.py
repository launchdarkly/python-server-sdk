from __future__ import division, with_statement, absolute_import

import hashlib
import hmac
import threading

import requests
from builtins import object

from ldclient.config import Config as Config
from ldclient.event_processor import NullEventProcessor
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.flag import evaluate
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

from cachecontrol import CacheControl
from threading import Lock


class LDClient(object):
    def __init__(self, sdk_key=None, config=None, start_wait=5):
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

        self._session = CacheControl(requests.Session())
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
        return self._config.sdk_key

    def close(self):
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
        self._sanitize_user(user)
        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when calling track().")
        self._send_event({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def identify(self, user):
        self._sanitize_user(user)
        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when calling identify().")
        self._send_event({'kind': 'identify', 'key': user.get('key'), 'user': user})

    def is_offline(self):
        return self._config.offline

    def is_initialized(self):
        return self.is_offline() or self._config.use_ldd or self._update_processor.initialized()

    def flush(self):
        if self._config.offline:
            return
        return self._event_processor.flush()

    def toggle(self, key, user, default):
        log.warn("Deprecated method: toggle() called. Use variation() instead.")
        return self.variation(key, user, default)

    def variation(self, key, user, default):
        default = self._config.get_default(key, default)
        if user is not None:
            self._sanitize_user(user)

        if self._config.offline:
            return default

        def send_event(value, version=None):
            self._send_event({'kind': 'feature', 'key': key, 'user': user, 'variation': None,
                              'value': value, 'default': default, 'version': version,
                              'trackEvents': False, 'debugEventsUntilDate': None})

        if not self.is_initialized():
            if self._store.initialized:
                log.warn("Feature Flag evaluation attempted before client has initialized - using last known values from feature store for feature key: " + key)
            else:
                log.warn("Feature Flag evaluation attempted before client has initialized! Feature store unavailable - returning default: "
                         + str(default) + " for feature key: " + key)
                send_event(default)
                return default

        if user is not None and user.get('key', "") == "":
            log.warn("User key is blank. Flag evaluation will proceed, but the user will not be stored in LaunchDarkly.")

        def cb(flag):
            try:
                if not flag:
                    log.info("Feature Flag key: " + key + " not found in Feature Store. Returning default.")
                    send_event(default)
                    return default

                return self._evaluate_and_send_events(flag, user, default)

            except Exception as e:
                log.error("Exception caught in variation: " + e.message + " for flag key: " + key + " and user: " + str(user))
                send_event(default)

            return default

        return self._store.get(FEATURES, key, cb)

    def _evaluate(self, flag, user):
        return evaluate(flag, user, self._store)

    def _evaluate_and_send_events(self, flag, user, default):
        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when evaluating Feature Flag key: " + flag.get('key') + ". Returning default.")
            value = default
            variation = None
        else:
            result = evaluate(flag, user, self._store)
            for event in result.events or []:
                self._send_event(event)
            value = default if result.value is None else result.value
            variation = result.variation
        
        self._send_event({'kind': 'feature', 'key': flag.get('key'),
                          'user': user, 'variation': variation, 'value': value,
                          'default': default, 'version': flag.get('version'),
                          'trackEvents': flag.get('trackEvents'),
                          'debugEventsUntilDate': flag.get('debugEventsUntilDate')})
        return value

    def all_flags(self, user):
        if self._config.offline:
            log.warn("all_flags() called, but client is in offline mode. Returning None")
            return None

        if not self.is_initialized():
            if self._store.initialized:
                log.warn("all_flags() called before client has finished initializing! Using last known values from feature store")
            else:
                log.warn("all_flags() called before client has finished initializing! Feature store unavailable - returning None")
                return None

        if user is None or user.get('key') is None:
            log.warn("User or user key is None when calling all_flags(). Returning None.")
            return None

        def cb(all_flags):
            try:
                return self._evaluate_multi(user, all_flags)
            except Exception as e:
                log.error("Exception caught in all_flags: " + e.message + " for user: " + str(user))
            return {}

        return self._store.all(FEATURES, cb)

    def _evaluate_multi(self, user, flags):
        return dict([(k, self._evaluate(v, user).value) for k, v in flags.items() or {}])

    def secure_mode_hash(self, user):
        if user.get('key') is None or self._config.sdk_key is None:
            return ""
        return hmac.new(self._config.sdk_key.encode(), user.get('key').encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def _sanitize_user(user):
        if 'key' in user:
            user['key'] = str(user['key'])


__all__ = ['LDClient', 'Config']
