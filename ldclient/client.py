from __future__ import division, with_statement, absolute_import

import hashlib
import hmac
import threading
import time

import requests
from builtins import object

from ldclient.event_consumer import EventConsumerImpl
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import evaluate
from ldclient.interfaces import FeatureStore
from ldclient.polling import PollingUpdateProcessor
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.util import check_uwsgi, log

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

from cachecontrol import CacheControl
from threading import Lock

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FEATURES_PATH = '/flags'


class Config(object):
    def __init__(self,
                 base_uri='https://app.launchdarkly.com',
                 events_uri='https://events.launchdarkly.com',
                 connect_timeout=2,
                 read_timeout=10,
                 events_upload_max_batch_size=100,
                 events_max_pending=10000,
                 stream_uri='https://stream.launchdarkly.com',
                 stream=True,
                 verify_ssl=True,
                 defaults=None,
                 events_enabled=True,
                 update_processor_class=None,
                 poll_interval=1,
                 use_ldd=False,
                 feature_store=InMemoryFeatureStore(),
                 feature_requester_class=None,
                 event_consumer_class=None,
                 offline=False):
        """

        :param update_processor_class: A factory for an UpdateProcessor implementation taking the sdk key, config,
                                       and FeatureStore implementation
        :type update_processor_class: (str, Config, FeatureStore) -> UpdateProcessor
        :param feature_store: A FeatureStore implementation
        :type feature_store: FeatureStore
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :type feature_requester_class: (str, Config, FeatureStore) -> FeatureRequester
        :param event_consumer_class: A factory for an EventConsumer implementation taking the event queue, sdk key, and config
        :type event_consumer_class: (queue.Queue, str, Config) -> EventConsumer
        """
        if defaults is None:
            defaults = {}

        self.base_uri = base_uri.rstrip('\\')
        self.get_latest_features_uri = self.base_uri + GET_LATEST_FEATURES_PATH
        self.events_uri = events_uri.rstrip('\\') + '/bulk'
        self.stream_uri = stream_uri.rstrip('\\') + STREAM_FEATURES_PATH
        self.update_processor_class = update_processor_class
        self.stream = stream
        if poll_interval < 1:
            poll_interval = 1
        self.poll_interval = poll_interval
        self.use_ldd = use_ldd
        self.feature_store = InMemoryFeatureStore() if not feature_store else feature_store
        self.event_consumer_class = EventConsumerImpl if not event_consumer_class else event_consumer_class
        self.feature_requester_class = feature_requester_class
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.events_enabled = events_enabled
        self.events_upload_max_batch_size = events_upload_max_batch_size
        self.events_max_pending = events_max_pending
        self.verify_ssl = verify_ssl
        self.defaults = defaults
        self.offline = offline

    def get_default(self, key, default):
        return default if key not in self.defaults else self.defaults[key]

    @classmethod
    def default(cls):
        return cls()


class LDClient(object):
    def __init__(self, sdk_key, config=None, start_wait=5):
        check_uwsgi()
        self._sdk_key = sdk_key
        self._config = config or Config.default()
        self._session = CacheControl(requests.Session())
        self._queue = queue.Queue(self._config.events_max_pending)
        self._event_consumer = None
        self._lock = Lock()

        self._store = self._config.feature_store
        """ :type: FeatureStore """

        if self._config.offline:
            self._config.events_enabled = False
            log.info("Started LaunchDarkly Client in offline mode")
            return

        if self._config.events_enabled:
            self._event_consumer = self._config.event_consumer_class(
                self._queue, self._sdk_key, self._config)
            self._event_consumer.start()

        if self._config.use_ldd:
            log.info("Started LaunchDarkly Client in LDD mode")
            return

        if self._config.feature_requester_class:
            self._feature_requester = self._config.feature_requester_class(
                sdk_key, self._config)
        else:
            self._feature_requester = FeatureRequesterImpl(sdk_key, self._config)
        """ :type: FeatureRequester """

        update_processor_ready = threading.Event()

        if self._config.update_processor_class:
            log.info("Using user-specified update processor: " + str(self._config.update_processor_class))
            self._update_processor = self._config.update_processor_class(
                sdk_key, self._config, self._feature_requester, self._store, update_processor_ready)
        else:
            if self._config.stream:
                self._update_processor = StreamingUpdateProcessor(
                    sdk_key, self._config, self._feature_requester, self._store, update_processor_ready)
            else:
                self._update_processor = PollingUpdateProcessor(
                    sdk_key, self._config, self._feature_requester, self._store, update_processor_ready)
        """ :type: UpdateProcessor """

        self._update_processor.start()
        log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
        update_processor_ready.wait(start_wait)

        if self._update_processor.initialized:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.info("Initialization timeout exceeded for LaunchDarkly Client. Feature Flags may not yet be available.")

    @property
    def sdk_key(self):
        return self._sdk_key

    def close(self):
        log.info("Closing LaunchDarkly client..")
        if self.is_offline():
            return
        if self._event_consumer and self._event_consumer.is_alive():
            self._event_consumer.stop()
        if self._update_processor and self._update_processor.is_alive():
            self._update_processor.stop()

    def _send_event(self, event):
        if self._config.offline or not self._config.events_enabled:
            return
        event['creationDate'] = int(time.time() * 1000)
        if self._queue.full():
            log.warning("Event queue is full-- dropped an event")
        else:
            self._queue.put(event)

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
        if self._config.offline or not self._config.events_enabled:
            return
        return self._event_consumer.flush()

    def toggle(self, key, user, default):
        log.warn("Deprecated method: toggle() called. Use variation() instead.")
        return self.variation(key, user, default)

    def variation(self, key, user, default):
        default = self._config.get_default(key, default)
        self._sanitize_user(user)

        if self._config.offline:
            return default

        def send_event(value, version=None):
            self._send_event({'kind': 'feature', 'key': key,
                              'user': user, 'value': value, 'default': default, 'version': version})

        if not self.is_initialized():
            log.warn("Feature Flag evaluation attempted before client has finished initializing! Returning default: "
                     + str(default) + " for feature key: " + key)
            send_event(default)
            return default

        if user is None or user.get('key') is None:
            log.warn("Missing user or user key when evaluating Feature Flag key: " + key + ". Returning default.")
            send_event(default)
            return default

        if user.get('key', "") == "":
            log.warn("User key is blank. Flag evaluation will proceed, but the user will not be stored in LaunchDarkly.")

        def cb(flag):
            try:
                if not flag:
                    log.warn("Feature Flag key: " + key + " not found in Feature Store. Returning default.")
                    send_event(default)
                    return default

                return self._evaluate_and_send_events(flag, user, default)

            except Exception as e:
                log.error("Exception caught in variation: " + e.message + " for flag key: " + key + " and user: " + str(user))

            return default

        return self._store.get(key, cb)

    def _evaluate(self, flag, user):
        return evaluate(flag, user, self._store)

    def _evaluate_and_send_events(self, flag, user, default):
        value, events = self._evaluate(flag, user)
        for event in events or []:
            self._send_event(event)

        if value is None:
            value = default
        self._send_event({'kind': 'feature', 'key': flag.get('key'),
                          'user': user, 'value': value, 'default': default, 'version': flag.get('version')})
        return value

    def all_flags(self, user):
        if self._config.offline:
            log.warn("all_flags() called, but client is in offline mode. Returning None")
            return None

        if not self.is_initialized():
            log.warn("all_flags() called before client has finished initializing! Returning None")
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

        return self._store.all(cb)

    def _evaluate_multi(self, user, flags):
        return {k: self._evaluate(v, user)[0] for k, v in flags.items() or {}}

    def secure_mode_hash(self, user):
        if user.get('key') is None:
            return ""
        return hmac.new(self._sdk_key.encode(), user.get('key').encode(), hashlib.sha256).hexdigest()

    @staticmethod
    def _sanitize_user(user):
        if 'key' in user:
            user['key'] = str(user['key'])


__all__ = ['LDClient', 'Config']
