from __future__ import division, with_statement
from builtins import object
import requests
import json
import hashlib
import logging
import time
import errno
import sys

try:
    import queue
except:
    import Queue as queue

from datetime import datetime, timedelta
from cachecontrol import CacheControl
from requests.packages.urllib3.exceptions import ProtocolError
from threading import Thread, Lock
from sseclient import SSEClient

from ldclient.version import VERSION
from ldclient.rwlock import ReadWriteLock

__version__ = VERSION

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email", "firstName", "lastName", "avatar", "name", "anonymous"]

log = logging.getLogger(__name__)

# Add a NullHandler for Python < 2.7 compatibility
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

if not log.handlers:
    log.addHandler(NullHandler())

try:
  unicode
except NameError:
  __BASE_TYPES__ = (str, float, int, bool)
else:
  __BASE_TYPES__ = (str, float, int, bool, unicode)

class Config(object):

    def __init__(self,
                 base_uri = 'https://app.launchdarkly.com',
                 connect_timeout = 2,
                 read_timeout = 10,
                 upload_limit = 100,
                 capacity = 10000,
                 stream_uri = 'https://stream.launchdarkly.com',
                 stream = False,
                 verify = True,
                 stream_processor_class = None,
                 feature_store_class = None):
        self._base_uri = base_uri.rstrip('\\')
        self._stream_uri = stream_uri.rstrip('\\')
        self._stream = stream
        self._stream_processor_class = StreamProcessor if not stream_processor_class else stream_processor_class
        self._feature_store_class = InMemoryFeatureStore if not feature_store_class else feature_store_class
        self._connect = connect_timeout
        self._read = read_timeout
        self._upload_limit = upload_limit
        self._capacity = capacity
        self._verify = verify

    @classmethod
    def default(cls):
        return cls()

class InMemoryFeatureStore(object):
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
            return dict((k,f) for k,f in self._features.iteritems() if ('deleted' not in f) or not f['deleted'])
        finally:
            self._lock.runlock()

    def init(self, features):
        try:
            self._lock.lock()
            self._features = dict(features)
            self._initialized = True
        finally:
            self._lock.unlock()

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

    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()

class StreamProcessor(Thread):
    def __init__(self, api_key, config):
        Thread.__init__(self)
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._store = config._feature_store_class()
        self._running = False

    def run(self):
        log.debug("Starting stream processor")
        self._running = True
        hdrs = _stream_headers(self._api_key)
        uri = self._config._stream_uri + "/"    
        messages = SSEClient(uri, verify = self._config._verify, headers = hdrs)
        for msg in messages:
            if not self._running:
                break
            self.process_message(self._store, msg)

    def initialized(self):
        return self._store.initialized()

    def get_feature(self, key):
        return self._store.get(key)

    def stop(self):
        self._running = False

    @staticmethod
    def process_message(store, msg):
        payload = json.loads(msg.data)
        if msg.event == 'put':
            store.init(payload)
        elif msg.event == 'patch':
            key = payload['path'][1:]
            feature = payload['data']
            store.upsert(key, feature)
        elif msg.event == 'delete':
            key = payload['path'][1:]
            version = payload['version']
            store.delete(key, version)
        else:
            log.warning('Unhandled event in stream processor: ' + msg.event)

class Consumer(Thread):
    def __init__(self, queue, api_key, config):
        Thread.__init__(self)
        self._session = requests.Session()
        self.daemon = True
        self._api_key = api_key
        self._config = config
        self._queue = queue

    def run(self):
        log.debug("Starting event consumer")
        self._running = True
        while self._running:
            self.send()

    def stop(self):
        self._running = False

    def send_batch(self, events):
        def do_send(should_retry):
            try: 
                if isinstance(events, dict):
                    body = [events]
                else:
                    body = events    
                hdrs = _headers(self._api_key)
                uri = self._config._base_uri + '/api/events/bulk'
                r = self._session.post(uri, headers = hdrs, timeout = (self._config._connect, self._config._read), data=json.dumps(body)) 
                r.raise_for_status()
            except ProtocolError as e:
                inner = e.args[1]
                if inner.errno == errno.ECONNRESET and should_retry:
                    log.warning('ProtocolError exception caught while sending events. Retrying.')
                    do_send(False)
                else:
                    log.exception('Unhandled exception in event consumer. Analytics events were not processed.')
            except:
                log.exception('Unhandled exception in event consumer. Analytics events were not processed.')
        try:
            do_send(True)            
        finally:
            for event in events:
                self._queue.task_done()


    def send(self):
        events = self.next()

        if len(events) == 0:
            return
        else:
            self.send_batch(events)

    def next(self):
        queue = self._queue
        items = []

        item = self.next_item()
        if item is None:
            return items

        items.append(item)
        while len(items) < self._config._upload_limit and not queue.empty():
            item = self.next_item()
            if item:
                items.append(item)

        return items

    def next_item(self):
        queue = self._queue
        try:
            item = queue.get(block=True, timeout=5)
            return item
        except Exception:
            return None

class LDClient(object):

    def __init__(self, api_key, config = None):
        check_uwsgi()
        self._api_key = api_key
        self._config = config or Config.default()
        self._session = CacheControl(requests.Session())
        self._queue = queue.Queue(self._config._capacity)
        self._consumer = None
        self._offline = False
        self._lock = Lock()
        self._stream_processor = None
        if self._config._stream:
            self._stream_processor = config._stream_processor_class(api_key, config)
            self._stream_processor.start()

    def _check_consumer(self):
        with self._lock:
            if not self._consumer or not self._consumer.is_alive():
                self._consumer = Consumer(self._queue, self._api_key, self._config)
                self._consumer.start()

    def _stop_consumers(self):
        if self._consumer and self._consumer.is_alive():
            self._consumer.stop()
        if self._stream_processor and self._stream_processor.is_alive():
            self._stream_processor.stop()

    def _send(self, event):
        if self._offline:
            return
        self._check_consumer()
        event['creationDate'] = int(time.time()*1000)
        if self._queue.full():
            log.warning("Event queue is full-- dropped an event")
        else:
            self._queue.put(event)

    def track(self, event_name, user, data = None):
        self._send({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def identify(self, user):
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
        self._queue.join()

    def get_flag(self, key, user, default=False):
        return self.toggle(key, user, default)

    def toggle(self, key, user, default=False):
        def do_toggle(should_retry):
            try:
                if self._offline:
                    return default
                val = self._toggle(key, user, default)
                self._send({'kind': 'feature', 'key': key, 'user': user, 'value': val})
                return val
            except ProtocolError as e:
                inner = e.args[1]
                if inner.errno == errno.ECONNRESET and should_retry:
                    log.warning('ProtocolError exception caught while getting flag. Retrying.')
                    do_toggle(False)
                else:
                    log.exception('Unhandled exception. Returning default value for flag.')
                    return default
            except Exception as e:
                log.exception('Unhandled exception. Returning default value for flag.')
                return default
        return do_toggle(True)

    def _toggle(self, key, user, default):
        if self._config._stream and self._stream_processor.initialized():
            feature = self._stream_processor.get_feature(key)
        else:
            hdrs = _headers(self._api_key)
            uri = self._config._base_uri + '/api/eval/features/' + key
            r = self._session.get(uri, headers=hdrs, timeout = (self._config._connect, self._config._read))
            r.raise_for_status()
            feature = r.json()
        val = _evaluate(feature, user)
        if val is None:
            return default
        return val

def _headers(api_key):
    return {'Authorization': 'api_key ' + api_key, 'User-Agent': 'PythonClient/' + __version__, 'Content-Type': "application/json"}

def _stream_headers(api_key, client="PythonClient"):
    return {'Authorization': 'api_key ' + api_key,
            'User-Agent': 'PythonClient/' + __version__,
            'Cache-Control': 'no-cache',
            'Accept': "text/event-stream"}

def _param_for_user(feature, user):
    if 'key' in user and user['key']:
        idHash = user['key']
    else:
        log.exception('User does not have a valid key set. Returning default value for flag.')
        return None
    if 'secondary' in user:
        idHash += "." + user['secondary']
    hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], idHash)
    hash_val = int(hashlib.sha1(hash_key.encode('utf-8')).hexdigest()[:15], 16)
    result = hash_val / __LONG_SCALE__
    return result

def _match_target(target, user):
    attr = target['attribute']
    if attr in __BUILTINS__:
        if attr in user:
            u_value = user[attr]
            return u_value in target['values']
        else:
            return False
    else:  # custom attribute
        if 'custom' not in user:
            return False
        if attr not in user['custom']:
            return False
        u_value = user['custom'][attr]
        if isinstance(u_value, __BASE_TYPES__):
            return u_value in target['values']
        elif isinstance(u_value, (list, tuple)):
            return len(set(u_value).intersection(target['values'])) > 0
        return False

def _match_user(variation, user):
    if 'userTarget' in variation:
        return _match_target(variation['userTarget'], user)
    return False

def _match_variation(variation, user):
    for target in variation['targets']:
        if 'userTarget' in variation and target['attribute'] == 'key':
            continue
        if _match_target(target, user):
            return True
    return False

def check_uwsgi():
    if 'uwsgi' in sys.modules:
        import uwsgi
        if not uwsgi.opt.get('enable-threads'):
            log.warning('The LaunchDarkly client requires the enable-threads option '
                            'be passed to uWSGI. If enable-threads is not provided, no '
                            'threads will run and event data will not be sent to LaunchDarkly. '
                            'To learn more, see http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference#configuring-uwsgi')


def _evaluate(feature, user):
    if feature is None:
        return None
    if not feature['on']:
        return None
    param = _param_for_user(feature, user)
    if param is None:
        return None

    for variation in feature['variations']:
        if _match_user(variation, user):
            return variation['value']

    for variation in feature['variations']:
        if _match_variation(variation, user):
            return variation['value']

    total = 0.0
    for variation in feature['variations']:
        total += float(variation['weight']) / 100.0
        if param < total:
            return variation['value']
    return None
