from __future__ import division, with_statement, absolute_import

import time

import requests
from builtins import object

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.interfaces import FeatureStore
from ldclient.polling import PollingUpdateProcessor
from ldclient.requester import RequestsEventConsumer, FeatureRequesterImpl

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
                 upload_limit=100,
                 capacity=10000,
                 stream_uri='https://stream.launchdarkly.com',
                 stream=True,
                 verify=True,
                 defaults=None,
                 events=True,
                 update_processor_class=None,
                 poll_interval=1,
                 use_ldd=False,
                 feature_store=InMemoryFeatureStore(),
                 feature_requester_class=FeatureRequesterImpl,
                 consumer_class=None):
        """

        :param update_processor_class: A factory for an UpdateProcessor implementation taking the api key, config,
                                       and FeatureStore implementation
        :type update_processor_class: (str, Config, FeatureStore) -> UpdateProcessor
        :param feature_store: A FeatureStore implementation
        :type feature_store: FeatureStore
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the api key and config
        :type feature_requester_class: (str, Config, FeatureStore) -> FeatureRequester
        :param consumer_class: A factory for an EventConsumer implementation taking the event queue, api key, and config
        :type consumer_class: (queue.Queue, str, Config) -> EventConsumer
        """
        if defaults is None:
            defaults = {}

        self.base_uri = base_uri.rstrip('\\')
        self.get_latest_features_uri = self.base_uri + GET_LATEST_FEATURES_PATH
        self.events_uri = events_uri.rstrip('\\')
        self.stream_uri = stream_uri.rstrip('\\')
        self.stream_features_uri = self.stream_uri + STREAM_FEATURES_PATH

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
        self.consumer_class = RequestsEventConsumer if not consumer_class else consumer_class
        self.feature_requester_class = FeatureRequesterImpl if not feature_requester_class else feature_requester_class
        self.connect = connect_timeout
        self.read_timeout = read_timeout
        self.upload_limit = upload_limit
        self.capacity = capacity
        self.verify = verify
        self.defaults = defaults
        self.events = events

    def get_default(self, key, default):
        return default if key not in self.defaults else self.defaults[key]

    @classmethod
    def default(cls):
        return cls()


class LDClient(object):
    def __init__(self, api_key, config=None):
        check_uwsgi()
        self._api_key = api_key
        self._config = config or Config.default()
        self._session = CacheControl(requests.Session())
        self._queue = queue.Queue(self._config.capacity)
        self._consumer = None
        self._offline = False
        self._lock = Lock()

        self._store = self._config.feature_store
        """ :type: FeatureStore """

        self._feature_requester = self._config.feature_requester_class(
            api_key, self._config)
        """ :type: FeatureRequester """

        self._update_processor = self._config.update_processor_class(
            api_key, self._config, self._feature_requester, self._store)
        """ :type: UpdateProcessor """

        self._update_processor.start()
        log.info("Started LaunchDarkly Client")

    @property
    def api_key(self):
        return self._api_key

    def _check_consumer(self):
        with self._lock:
            if not self._consumer or not self._consumer.is_alive():
                self._consumer = self._config.consumer_class(
                    self._queue, self._api_key, self._config)
                self._consumer.start()

    def _stop_consumers(self):
        if self._consumer and self._consumer.is_alive():
            self._consumer.stop()
        if self._update_processor and self._update_processor.is_alive():
            self._update_processor.stop()

    def _send(self, event):
        if self._offline or not self._config.events:
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

    def set_offline(self):
        self._offline = True
        self._stop_consumers()

    def set_online(self):
        self._offline = False
        self._check_consumer()

    def is_offline(self):
        return self._offline

    def flush(self):
        if self._offline:
            return
        self._check_consumer()
        return self._consumer.flush()

    def get_flag(self, key, user, default=False):
        return self.toggle(key, user, default)

    def toggle(self, key, user, default=False):
        default = self._config.get_default(key, default)

        def send_event(value):
            self._send({'kind': 'feature', 'key': key,
                        'user': user, 'value': value, 'default': default})

        if self._offline:
            send_event(default)
            return default

        self._sanitize_user(user)

        if 'key' in user and user['key']:
            feature = self._store.get(key)
        else:
            # log warning?
            send_event(default)
            return default

        val = _evaluate(feature, user)
        if val is None:
            # log warning?
            send_event(default)
            return default

        send_event(val)
        return val

    def _sanitize_user(self, user):
        if 'key' in user:
            user['key'] = str(user['key'])

__all__ = ['LDClient', 'Config']
