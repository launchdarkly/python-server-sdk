import requests
import json
import hashlib

from cachecontrol import CacheControl

__version__ = "0.1"

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

class Config(object):

  def __init__(self, baseUri):
    self._baseUri = baseUri

  @classmethod
  def default(cls):
    return cls('https://app.launchdarkly.com')

class LDClient(object):

  def __init__(self, apiKey, config = Config.default()):
    self._apiKey = apiKey
    self._config = config
    self._session = CacheControl(requests.Session())

  def get_flag(self, key, user, default = False):
    h = {'Authorization': 'api_key ' + self._apiKey,
          'User-Agent': 'PythonClient/' + __version__}
    r = self._session.get(self._config._baseUri + '/api/eval/features/' + key, headers = h)
    dict = r.json()
    val = evaluate(dict, user)
    if (val is None):
      return default
    else:
      return val

def param_for_user(dict, user): 
  if ('key' in user):
    idHash = user['key']
  else:
    return None
  if ('secondary' in user):
    idHash += "." + user['secondary']
  hash = long(hashlib.sha1('%s.%s.%s' % (dict['key'], dict['salt'], idHash)).hexdigest()[:15], 16)
  result = hash / __LONG_SCALE__
  return result;

def match_target(target, user):
  attr = target['attribute']
  if (attr == 'key' or attr == 'ip' or attr == 'country' and attr in user):      
    u_value = user[attr]
    return u_value in target['values']
  else: # custom attribute
    if ('custom' not in user):
      return False
    if (attr not in user['custom']):
      return False
    u_value = user['custom'][attr]
    if (isinstance(u_value, str) or isinstance(u_value, (float, int, long))):
      return u_value in target['values']
    elif (isinstance(u_value, collections.Sequence)):
      return len(set(u_value).intersection(target['values'])) > 0
    return False


def match_variation(variation, user):
  for target in variation['targets']:
    if match_target(target, user):
      return True
  return False

def evaluate(dict, user):
  if (not dict['on']): 
    return None
  param = param_for_user(dict, user)
  if (param is None):
    return None

  for variation in dict['variations']:
    if (match_variation(variation, user)):
      return variation['value']

  sum = 0.0
  for variation in dict['variations']:
    sum += float(variation['weight']) / 100.0
    if (param < sum):
      return variation['value']

  return False





