from __future__ import division, with_statement, absolute_import

import certifi
import logging
import sys
import urllib3

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
            'User-Agent': '{0}/{1}'.format(client, VERSION),
            'Cache-Control': 'no-cache',
            'Accept': "text/event-stream"}


def check_uwsgi():
    if 'uwsgi' in sys.modules:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import uwsgi

        if uwsgi.opt.get('enable-threads'):
            return
        if uwsgi.opt.get('threads') is not None and int(uwsgi.opt.get('threads')) > 1:
            return
        log.error("The LaunchDarkly client requires the 'enable-threads' or 'threads' option be passed to uWSGI. "
                    'To learn more, see http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference#configuring-uwsgi')


class Event(object):

    def __init__(self, data='', event='message', event_id=None, retry=None):
        self.data = data
        self.event = event
        self.id = event_id
        self.retry = retry

    def __str__(self, *args, **kwargs):
        return self.data


class UnsuccessfulResponseException(Exception):
    def __init__(self, status):
        super(UnsuccessfulResponseException, self).__init__("HTTP error %d" % status)
        self._status = status

    @property
    def status(self):
        return self._status


def create_http_pool_manager(num_pools=1, verify_ssl=False):
    if not verify_ssl:
        return urllib3.PoolManager(num_pools=num_pools)
    return urllib3.PoolManager(
        num_pools=num_pools,
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where()
        )


def throw_if_unsuccessful_response(resp):
    if resp.status >= 400:
        raise UnsuccessfulResponseException(resp.status)


def is_http_error_recoverable(status):
    if status >= 400 and status < 500:
        return (status == 400) or (status == 408) or (status == 429)  # all other 4xx besides these are unrecoverable
    return True  # all other errors are recoverable


def http_error_message(status, context, retryable_message = "will retry"):
    return "Received HTTP error %d%s for %s - %s" % (
        status,
        " (invalid SDK key)" if (status == 401 or status == 403) else "",
        context,
        retryable_message if is_http_error_recoverable(status) else "giving up permanently"
        )
