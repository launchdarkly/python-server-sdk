import requests
import json
import hashlib
import logging

from cachecontrol import CacheControl

__version__ = "0.3"

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

class Config(object):

    def __init__(self, baseUri):
        self._baseUri = baseUri

    @classmethod
    def default(cls):
        return cls('https://app.launchdarkly.com')


class LDClient(object):

    def __init__(self, apiKey, config=Config.default()):
        self._apiKey = apiKey
        self._config = config
        self._session = CacheControl(requests.Session())

    def get_flag(self, key, user, default=False):
        try:
            return self._get_flag(key, user, default)
        except:
            logging.exception('Unhandled exception in get_flag. Returning default value for flag.')
            return default

    def _get_flag(self, key, user, default):
        hdrs = {'Authorization': 'api_key ' + self._apiKey,
             'User-Agent': 'PythonClient/' + __version__}
        uri = self._config._baseUri + '/api/eval/features/' + key
        r = self._session.get(uri, headers=hdrs)
        dict = r.json()
        val = _evaluate(dict, user)
        if val is None:
            return default
        else:
            return val

def _param_for_user(feature, user):
    if 'key' in user:
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

    return False
