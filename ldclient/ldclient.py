import requests
import json
import hashlib

from cachecontrol import CacheControl

__version__ = "0.1"

class Config(object):

  def __init__(self, baseUri):
    self._baseUri = baseUri

  @classmethod
  def default(cls):
    return cls('https://app.launchdarkly.com')

class LdClient(object):

  def __init__(self, apiKey, config = Config.default()):
    self._apiKey = apiKey
    self._config = config
    self._session = CacheControl(requests.Session())

  def get_flag(self, key, user, default = False):
    h = {'Authorization': 'api_key ' + self._apiKey,
          'User-Agent', 'PythonClient/' + __version__}
    r = self._session.get(self._config._baseUri + '/api/eval/features/' + key, headers = h)
    dict = r.json()
    if (not dict['on']): 
      return false
    val = evaluate(dict, user)
    if (val is None or user is None):
      return default
    else:
      return val

  def param_for_user(dict, user): 
    if (user['key'] is not None):
      idHash = user['key']
    else:
      return None
    if (user['secondary'] is not None):
      idHash += "." + user['secondary']
    hash = hashlib.sha1(dict['key'] + '.' + dict['salt'] + '.' + idHash)[0:15]

  def evaluate(dict, user):
    return False





