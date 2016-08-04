from __future__ import division, with_statement, absolute_import

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

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

try:
    # noinspection PyUnresolvedReferences
    unicode
except NameError:
    __BASE_TYPES__ = (str, float, int, bool)
else:
    # noinspection PyUnresolvedReferences
    __BASE_TYPES__ = (str, float, int, bool, unicode)


def _headers(sdk_key):
    return {'Authorization': sdk_key, 'User-Agent': 'PythonClient/' + VERSION,
            'Content-Type': "application/json"}


def _stream_headers(sdk_key, client="PythonClient"):
    return {'Authorization': sdk_key,
            'User-Agent': '{}/{}'.format(client, VERSION),
            'Cache-Control': 'no-cache',
            'Accept': "text/event-stream"}


def check_uwsgi():
    if 'uwsgi' in sys.modules:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import uwsgi

        if not uwsgi.opt.get('enable-threads'):
            log.error('The LaunchDarkly client requires the enable-threads option be passed to uWSGI. '
                        'To learn more, see http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference#configuring-uwsgi')


class Event(object):

    def __init__(self, data='', event='message', event_id=None, retry=None):
        self.data = data
        self.event = event
        self.id = event_id
        self.retry = retry

    def __str__(self, *args, **kwargs):
        return self.data
