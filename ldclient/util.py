"""
General internal helper functions.
"""
# currently excluded from documentation - see docs/README.md

import certifi
import logging
from os import environ
import six
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

_retryable_statuses = [400, 408, 429]

def _base_headers(config):
    headers = {'Authorization': config.sdk_key,
               'User-Agent': 'PythonClient/' + VERSION}
    if isinstance(config.wrapper_name, str) and config.wrapper_name != "":
        wrapper_version = ""
        if isinstance(config.wrapper_version, str) and config.wrapper_version != "":
            wrapper_version = "/" + config.wrapper_version
        headers.update({'X-LaunchDarkly-Wrapper': config.wrapper_name + wrapper_version})
    return headers

def _headers(config):
    base_headers = _base_headers(config)
    base_headers.update({'Content-Type': "application/json"})
    return base_headers

def _stream_headers(config):
    base_headers = _base_headers(config)
    base_headers.update({ 'Cache-Control': "no-cache"
                        , 'Accept': "text/event-stream" })
    return base_headers

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


def create_http_pool_manager(num_pools=1, verify_ssl=False, target_base_uri=None, force_proxy=None):
    proxy_url = force_proxy or _get_proxy_url(target_base_uri)

    if not verify_ssl:
        if proxy_url is None:
            return urllib3.PoolManager(num_pools=num_pools)
        else:
            return urllib3.ProxyManager(proxy_url, num_pools=num_pools)
    
    if proxy_url is None:
        return urllib3.PoolManager(
            num_pools=num_pools,
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
            )
    else:
        return urllib3.ProxyManager(
            proxy_url,
            num_pools=num_pools,
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )

def _get_proxy_url(target_base_uri):
    if target_base_uri is None:
        return None
    is_https = target_base_uri.startswith('https:')
    if is_https:
        return environ.get('https_proxy')
    return environ.get('http_proxy')


def throw_if_unsuccessful_response(resp):
    if resp.status >= 400:
        raise UnsuccessfulResponseException(resp.status)


def is_http_error_recoverable(status):
    if status >= 400 and status < 500:
        return status in _retryable_statuses # all other 4xx besides these are unrecoverable
    return True  # all other errors are recoverable


def http_error_message(status, context, retryable_message = "will retry"):
    return "Received HTTP error %d%s for %s - %s" % (
        status,
        " (invalid SDK key)" if (status == 401 or status == 403) else "",
        context,
        retryable_message if is_http_error_recoverable(status) else "giving up permanently"
        )


def stringify_attrs(attrdict, attrs):
    if attrdict is None:
        return None
    newdict = None
    for attr in attrs:
        val = attrdict.get(attr)
        if val is not None and not isinstance(val, six.string_types):
            if newdict is None:
                newdict = attrdict.copy()
            newdict[attr] = str(val)
    return attrdict if newdict is None else newdict
