from __future__ import division, with_statement, absolute_import

import time

import requests
from builtins import object

from ldclient.event_consumer import EventConsumerImpl
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureStore
from ldclient.polling import PollingUpdateProcessor
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.util import check_uwsgi, _evaluate, log

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

from cachecontrol import CacheControl
from threading import Lock

GET_LATEST_FEATURES_PATH = '/api/eval/latest-features'
STREAM_FEATURES_PATH = '/features'


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
                 feature_requester_class=FeatureRequesterImpl,
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

        if update_processor_class:
            self.update_processor_class = update_processor_class
        else:
            if stream:
                self.update_processor_class = StreamingUpdateProcessor
            else:
                self.update_processor_class = PollingUpdateProcessor

        if poll_interval < 1:
            poll_interval = 1
        self.poll_interval = poll_interval
        self.use_ldd = use_ldd
        self.feature_store = feature_store
        self.event_consumer_class = EventConsumerImpl if not event_consumer_class else event_consumer_class
        self.feature_requester_class = FeatureRequesterImpl if not feature_requester_class else feature_requester_class
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

        self._feature_requester = self._config.feature_requester_class(
            api_key, self._config)
        """ :type: FeatureRequester """

        self._update_processor = self._config.update_processor_class(
            api_key, self._config, self._feature_requester, self._store)
        """ :type: UpdateProcessor """

        if self._config.offline:
            log.info("Started LaunchDarkly Client in offline mode")
            return

        start_time = time.time()
        self._update_processor.start()
        while not self._update_processor.initialized():
            if time.time() - start_time > start_wait:
                log.warn("Timeout encountered waiting for LaunchDarkly Client initialization")
                return
            time.sleep(0.1)

        log.info("Started LaunchDarkly Client")

    @property
    def api_key(self):
        return self._api_key

    def _check_consumer(self):
        with self._lock:
            if not self._event_consumer or not self._event_consumer.is_alive():
                self._event_consumer = self._config.event_consumer_class(
                    self._queue, self._api_key, self._config)
                self._event_consumer.start()

    def _stop_consumers(self):
        if self._event_consumer and self._event_consumer.is_alive():
            self._event_consumer.stop()
        if self._update_processor and self._update_processor.is_alive():
            self._update_processor.stop()

    def _send(self, event):
        if self._config.offline or not self._config.events_enabled:
            return
        self._check_consumer()
        event['creationDate'] = int(time.time() * 1000)
        if self._queue.full():
            log.warning("Event queue is full-- dropped an event")
        else:
            self._queue.put(event)

    def track(self, event_name, user, data=None):
        self._sanitize_user(user)
        self._send({'kind': 'custom', 'key': event_name,
                    'user': user, 'data': data})

    def identify(self, user):
        self._sanitize_user(user)
        self._send({'kind': 'identify', 'key': user['key'], 'user': user})

    def is_offline(self):
        return self._config.offline

    def flush(self):
        if self._config.offline:
            return
        self._check_consumer()
        return self._event_consumer.flush()

    def get_flag(self, key, user, default=False):
        return self.toggle(key, user, default)

    def toggle(self, key, user, default=False):
        default = self._config.get_default(key, default)

        def send_event(value):
            self._send({'kind': 'feature', 'key': key,
                        'user': user, 'value': value, 'default': default})

        if self._config.offline:
            return default

        self._sanitize_user(user)

        if 'key' in user and user['key']:
            feature = self._store.get(key)
        else:
            send_event(default)
            log.warning("Missing or empty User key when evaluating Feature Flag key: " + key + ". Returning default.")
            return default

        if feature:
            val = _evaluate(feature, user)
        else:
            log.warning("Feature Flag key: " + key + " not found in Feature Store. Returning default.")
            send_event(default)
            return default

        if val is None:
            send_event(default)
            log.warning("Feature Flag key: " + key + " evaluation returned None. Returning default.")
            return default

        send_event(val)
        return val

    def _sanitize_user(self, user):
        if 'key' in user:
            user['key'] = str(user['key'])

__all__ = ['LDClient', 'Config']
