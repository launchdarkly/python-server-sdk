import requests
import json
import hashlib
import logging
import time
import threading

from cachecontrol import CacheControl
from collections import deque

__version__ = "0.10"

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

class Consumer(object):
    def __init__(self):
        self._session = requests.Session()

    def send(self, events, api_key, base_uri, connect_timeout, read_timeout):
        try: 
            if isinstance(events, dict):
                body = [events]
            else:
                body = events    
            hdrs = _headers(api_key)
            uri = base_uri + '/api/events/bulk'
            r = self._session.post(uri, headers = hdrs, timeout = (connect_timeout, read_timeout), data=json.dumps(body))
            r.raise_for_status()
        except:
            logging.exception('Unhandled exception in consumer. Analytics events were not processed.')    

class Config(object):

    def __init__(self, base_uri, connect_timeout = 2, read_timeout = 10):
        self._base_uri = base_uri
        self._connect = connect_timeout
        self._read = read_timeout

    @classmethod
    def default(cls):
        return cls('https://app.launchdarkly.com')

class LDClient(object):

    def __init__(self, api_key, config = Config.default(), consumer = Consumer()):
        self._api_key = api_key
        self._config = config
        self._session = CacheControl(requests.Session())
        self._consumer = consumer

    def _send(self, event):
        event['creationDate'] = int(time.time()*1000)
        self._consumer.send(event, self._api_key, self._config._base_uri, self._config._connect, self._config._read)

    def send_event(self, event_name, user, data):
        self._send({'kind': 'custom', 'key': event_name, 'user': user, 'data': data})

    def get_flag(self, key, user, default=False):
        try:
            val = self._get_flag(key, user, default)
            self._send({'kind': 'feature', 'key': key, 'user': user, 'value': val})
            return val
        except:
            logging.exception('Unhandled exception in get_flag. Returning default value for flag.')
            return default

    def _get_headers(self):
        return {'Authorization': 'api_key ' + self._api_key,
             'User-Agent': 'PythonClient/' + __version__}

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
    return {'Authorization': 'api_key ' + api_key, 'User-Agent': 'PythonClient/' + __version__}

def _param_for_user(feature, user):
    if 'key' in user and user['key']:
        idHash = user['key']
    else:
        return None
    if 'secondary' in user:
        idHash += "." + user['secondary']
    hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], idHash)
    hash_val = long(hashlib.sha1(hash_key).hexdigest()[:15], 16)
    result = hash_val / __LONG_SCALE__
    return result


def _match_target(target, user):
    attr = target['attribute']
    if attr == 'key' or attr == 'ip' or attr == 'country':
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


def _match_variation(variation, user):
    for target in variation['targets']:
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
        if _match_variation(variation, user):
            return variation['value']

    total = 0.0
    for variation in feature['variations']:
        total += float(variation['weight']) / 100.0
        if param < total:
            return variation['value']

    return None
