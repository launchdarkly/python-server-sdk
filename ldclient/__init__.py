import requests
import json
import hashlib
import logging
import time
import threading

from datetime import datetime, timedelta
from cachecontrol import CacheControl
from collections import deque

__version__ = "0.14.0"

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email", "firstName", "lastName", "avatar", "name"]

class Config(object):

    def __init__(self, base_uri, connect_timeout = 2, read_timeout = 10):
        self._base_uri = base_uri.rstrip('\\')
        self._connect = connect_timeout
        self._read = read_timeout

    @classmethod
    def default(cls):
        return cls('https://app.launchdarkly.com')

class Consumer(object):
    def __init__(self, api_key, config = Config.default()):
        self._session = requests.Session()
        self._config = config
        self._api_key = api_key

    def send(self, events):
        try: 
            if isinstance(events, dict):
                body = [events]
            else:
                body = events    
            hdrs = _headers(self._api_key)
            uri = self._config._base_uri + '/api/events/bulk'
            r = self._session.post(uri, headers = hdrs, timeout = (self._config._connect, self._config._read), data=json.dumps(body))
            r.raise_for_status()
        except:
            logging.exception('Unhandled exception in consumer. Analytics events were not processed.')    

class AbstractBufferedConsumer(object):
    def __init__(self, capacity, interval):
        self._capacity = capacity
        self.queue = deque([], capacity) 
        self.last_flush = datetime.now()
        self._interval = interval

    def send(self, events):
        if isinstance(events, dict):
            self.queue.append(events)
        else:
            self.queue.extend(events)
        if self._should_flush():
            self.flush()

    def _should_flush(self):
        now = datetime.now()
        if self.last_flush + timedelta(seconds=self._interval) < now:
            return True
        if len(self.queue) >= self._capacity:
            return True
        return False

    def do_send(self, events):
        raise error("Unimplemented")

    def flush(self):
        to_process = []
        self.last_flush = datetime.now()
        while True:
            try:
                to_process.append(self.queue.popleft())
            except IndexError:
                break
        if to_process:
            self.do_send(to_process)


class BufferedConsumer(AbstractBufferedConsumer):
    def __init__(self, consumer, capacity = 500, interval = 5):
        self._consumer = consumer
        super(BufferedConsumer, self).__init__(capacity, interval)

    def do_send(self, events):
        self._consumer.send(events)

class AsyncConsumer(object):
    def __init__(self, consumer):
        self._consumer = consumer

    def send(self, events):
        t = threading.Thread(target=self._consumer.send, kwargs = {"events": events })
        t.daemon = True
        t.start()


class LDClient(object):

    def __init__(self, api_key, config = None, consumer = None):
        self._api_key = api_key
        self._config = config or Config.default()
        self._session = CacheControl(requests.Session())
        self._consumer = consumer or BufferedConsumer(AsyncConsumer(Consumer(api_key, config)))
        self._offline = False

    def _send(self, event):
        if self._offline:
            return
        event['creationDate'] = int(time.time()*1000)
        self._consumer.send(event)

    def track(self, event_name, user, data):
        self._send({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def identify(self, user):
        self._send({'kind': 'identify', 'key': user['key'], 'user': user})

    def set_offline(self):
        self._offline = true

    def set_online(self):
        self._offline = false

    def is_offline(self):
        return self._offline

    def get_flag(self, key, user, default=False):
        try:
            if self._offline:
                return default
            val = self._get_flag(key, user, default)
            self._send({'kind': 'feature', 'key': key, 'user': user, 'value': val})
            return val
        except:
            logging.exception('Unhandled exception in get_flag. Returning default value for flag.')
            return default

    def _get_flag(self, key, user, default):
        hdrs = _headers(self._api_key)
        uri = self._config._base_uri + '/api/eval/features/' + key
        r = self._session.get(uri, headers=hdrs, timeout = (self._config._connect, self._config._read))
        r.raise_for_status()
        hash = r.json()
        val = _evaluate(hash, user)
        if val is None:
            return default
        return val

def _headers(api_key):
    return {'Authorization': 'api_key ' + api_key, 'User-Agent': 'PythonClient/' + __version__, 'Content-Type': "application/json"}

def _param_for_user(feature, user):
    if 'key' in user and user['key']:
        idHash = user['key']
    else:
        logging.exception('User does not have a valid key set. Returning default value for flag.')
        return None
    if 'secondary' in user:
        idHash += "." + user['secondary']
    hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], idHash)
    hash_val = long(hashlib.sha1(hash_key).hexdigest()[:15], 16)
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
        if isinstance(u_value, str) or isinstance(u_value, (float, int, long)):
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


def _evaluate(feature, user):
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
