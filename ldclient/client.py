from __future__ import division, with_statement, absolute_import

import threading
import time

import requests
from builtins import object

from ldclient.event_consumer import EventConsumerImpl
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import _get_off_variation, _evaluate_index, _get_variation, evaluate
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

        :param update_processor_class: A factory for an UpdateProcessor implementation taking the api key, config,
                                       and FeatureStore implementation
        :type update_processor_class: (str, Config, FeatureStore) -> UpdateProcessor
        :param feature_store: A FeatureStore implementation
        :type feature_store: FeatureStore
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the api key and config
        :type feature_requester_class: (str, Config, FeatureStore) -> FeatureRequester
        :param event_consumer_class: A factory for an EventConsumer implementation taking the event queue, api key, and config
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
    def __init__(self, api_key, config=None, start_wait=5):
        check_uwsgi()
        self._api_key = api_key
        self._config = config or Config.default()
        self._session = CacheControl(requests.Session())
        self._queue = queue.Queue(self._config.events_max_pending)
        self._event_consumer = None
        self._lock = Lock()

        self._store = self._config.feature_store
        """ :type: FeatureStore """

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")
            return

        if self._config.events_enabled:
            self._event_consumer = self._config.event_consumer_class(
                self._queue, self._api_key, self._config)
            self._event_consumer.start()

        if self._config.use_ldd:
            if self._store.__class__ == "RedisFeatureStore":
                log.info("Started LaunchDarkly Client in LDD mode")
                return
            log.error("LDD mode requires a RedisFeatureStore.")
            return

        if self._config.feature_requester_class:
            self._feature_requester = self._config.feature_requester_class(
                api_key, self._config)
        else:
            self._feature_requester = FeatureRequesterImpl(api_key, self._config)
        """ :type: FeatureRequester """

        update_processor_ready = threading.Event()

        if self._config.update_processor_class:
            self._update_processor = self._config.update_processor_class(
                api_key, self._config, self._feature_requester, self._store, update_processor_ready)
        else:
            if self._config.stream:
                self._update_processor = StreamingUpdateProcessor(
                    api_key, self._config, self._feature_requester, self._store, update_processor_ready)
            else:
                self._update_processor = PollingUpdateProcessor(
                    api_key, self._config, self._feature_requester, self._store, update_processor_ready)
        """ :type: UpdateProcessor """

        self._update_processor.start()
        log.info("Waiting up to " + str(start_wait) + " seconds for LaunchDarkly client to initialize...")
        update_processor_ready.wait(start_wait)

        if self._update_processor.initialized:
            log.info("Started LaunchDarkly Client: OK")
        else:
            log.info("Initialization timeout exceeded for LaunchDarkly Client. Feature Flags may not yet be available.")

    @property
    def api_key(self):
        return self._api_key

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
        self._send_event({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def identify(self, user):
        self._sanitize_user(user)
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
        default = self._config.get_default(key, default)
        self._sanitize_user(user)

        if self._config.offline:
            return default

        def send_event(value):
            self._send_event({'kind': 'feature', 'key': key,
                              'user': user, 'value': value, 'default': default})

        if not self.is_initialized():
            log.warn("Feature Flag evaluation attempted before client has finished initializing! Returning default: "
                     + str(default) + " for feature key: " + key)
            send_event(default)
            return default

        if user.get('key', "") == "":
            log.warn("Missing or empty User key when evaluating Feature Flag key: " + key + ". Returning default.")
            send_event(default)
            return default

        flag = self._store.get(key)
        if not flag:
            log.warn("Feature Flag key: " + key + " not found in Feature Store. Returning default.")
            send_event(default)
            return default

        if flag.get('on', False):
            value, prereq_events = evaluate(flag, user, self._store)
            if not self._config.offline:
                for e in prereq_events:
                    self._send_event(e)

            if value is not None:
                send_event(value)
                return value

        if 'offVariation' in flag and flag['offVariation']:
                value = _get_variation(flag, flag['offVariation'])
                send_event(value)
                return value

        send_event(default)
        return default


    def _sanitize_user(self, user):
        if 'key' in user:
            user['key'] = str(user['key'])


__all__ = ['LDClient', 'Config']
