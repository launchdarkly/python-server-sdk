from __future__ import division, with_statement, absolute_import
from builtins import object
import time

from ldclient.interfaces import FeatureStore
from ldclient.requests import RequestsStreamProcessor, RequestsEventConsumer, RequestsFeatureRequester
from ldclient.util import check_uwsgi, _evaluate, log
import requests


# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

from cachecontrol import CacheControl
from threading import Lock

from ldclient.rwlock import ReadWriteLock


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
                 stream_processor_class=None,
                 feature_store_class=None,
                 feature_requester_class=None,
                 consumer_class=None):
        """

        :param stream_processor_class: A factory for a StreamProcessor implementation taking the api key, config,
                                       and FeatureStore implementation
        :type stream_processor_class: (str, Config, FeatureStore) -> StreamProcessor
        :param feature_store_class: A factory for a FeatureStore implementation
        :type feature_store_class: () -> FeatureStore
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the api key and config
        :type feature_requester_class: (str, Config) -> FeatureRequester
        :param consumer_class: A factory for an EventConsumer implementation taking the event queue, api key, and config
        :type consumer_class: (queue.Queue, str, Config) -> EventConsumer
        """
        if defaults is None:
            defaults = {}

        self.base_uri = base_uri.rstrip('\\')
        self.events_uri = events_uri.rstrip('\\')
        self.stream_uri = stream_uri.rstrip('\\')
        self.stream = stream
        self.stream_processor_class = RequestsStreamProcessor if not stream_processor_class else stream_processor_class
        self.feature_store_class = InMemoryFeatureStore if not feature_store_class else feature_store_class
        self.consumer_class = RequestsEventConsumer if not consumer_class else consumer_class
        self.feature_requester_class = RequestsFeatureRequester if not feature_requester_class else \
            feature_requester_class
        self.connect = connect_timeout
        self.read = read_timeout
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


class InMemoryFeatureStore(FeatureStore):

    def __init__(self):
        self._lock = ReadWriteLock()
        self._initialized = False
        self._features = {}

    def get(self, key):
        try:
            self._lock.rlock()
            f = self._features.get(key)
            if f is None or 'deleted' in f and f['deleted']:
                return None
            return f
        finally:
            self._lock.runlock()

    def all(self):
        try:
            self._lock.rlock()
            return dict((k, f) for k, f in self._features.items() if ('deleted' not in f) or not f['deleted'])
        finally:
            self._lock.runlock()

    def init(self, features):
        try:
            self._lock.lock()
            self._features = dict(features)
            self._initialized = True
        finally:
            self._lock.unlock()

    # noinspection PyShadowingNames
    def delete(self, key, version):
        try:
            self._lock.lock()
            f = self._features.get(key)
            if f is not None and f['version'] < version:
                f['deleted'] = True
                f['version'] = version
            elif f is None:
                f = {'deleted': True, 'version': version}
                self._features[key] = f
        finally:
            self._lock.unlock()

    def upsert(self, key, feature):
        try:
            self._lock.lock()
            f = self._features.get(key)
            if f is None or f['version'] < feature['version']:
                self._features[key] = f
        finally:
            self._lock.unlock()

    @property
    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()


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

        self._store = self._config.feature_store_class()
        """ :type: FeatureStore """

        self._feature_requester = self._config.feature_requester_class(
            api_key, self._config)
        """ :type: FeatureRequester """

        self._stream_processor = None
        if self._config.stream:
            self._stream_processor = self._config.stream_processor_class(
                api_key, self._config, self._store)
            self._stream_processor.start()

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
        if self._stream_processor and self._stream_processor.is_alive():
            self._stream_processor.stop()

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
        self._sanitize_user(user)
        default = self._config.get_default(key, default)

        if self._offline:
            return default

        def cb(feature):
            if feature is None:
                val = default
            else:
                val = _evaluate(feature, user)
                if val is None:
                    val = default
            self._send({'kind': 'feature', 'key': key,
                        'user': user, 'value': val, 'default': default})
            return val

        if self._config.stream and self._store.initialized:
            return cb(self._store.get(key))
        else:
            # noinspection PyBroadException
            try:
                return self._feature_requester.get(key, cb)
            except Exception:
                log.exception(
                    'Unhandled exception. Returning default value for flag.')
                return cb(None)

    def _sanitize_user(self, user):
        if 'key' in user:
            user['key'] = str(user['key'])

__all__ = ['LDClient', 'Config']
