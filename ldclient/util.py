from __future__ import division, with_statement, absolute_import
import hashlib
import logging
import sys

from ldclient.version import VERSION

log = logging.getLogger(sys.modules[__name__].__name__)


# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue


__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email", "firstName", "lastName", "avatar", "name", "anonymous"]

try:
    # noinspection PyUnresolvedReferences
    unicode
except NameError:
    __BASE_TYPES__ = (str, float, int, bool)
else:
    # noinspection PyUnresolvedReferences
    __BASE_TYPES__ = (str, float, int, bool, unicode)


def _headers(api_key):
    return {'Authorization': 'api_key ' + api_key, 'User-Agent': 'PythonClient/' + VERSION,
            'Content-Type': "application/json"}


def _stream_headers(api_key, client="PythonClient"):
    return {'Authorization': 'api_key ' + api_key,
            'User-Agent': '{}/{}'.format(client, VERSION),
            'Cache-Control': 'no-cache',
            'Accept': "text/event-stream"}


def _param_for_user(feature, user):
    if 'key' in user and user['key']:
        id_hash = user['key']
    else:
        log.exception('User does not have a valid key set. Returning default value for flag.')
        return None
    if 'secondary' in user:
        id_hash += "." + user['secondary']
    hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], id_hash)
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
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import uwsgi

        if not uwsgi.opt.get('enable-threads'):
            log.warning('The LaunchDarkly client requires the enable-threads option '
                        'be passed to uWSGI. If enable-threads is not provided, no '
                        'threads will run and event data will not be sent to LaunchDarkly. '
                        'To learn more, see '
                        'http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference#configuring-uwsgi')


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


class Event(object):
    def __init__(self, data='', event='message', event_id=None, retry=None):
        self.data = data
        self.event = event
        self.id = event_id
        self.retry = retry

    def __str__(self, *args, **kwargs):
        return self.data
