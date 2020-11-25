"""
General internal helper functions.
"""
# currently excluded from documentation - see docs/README.md

import logging
from os import environ
import sys
import urllib3

from ldclient.impl.http import HTTPFactory, _base_headers

log = logging.getLogger(sys.modules[__name__].__name__)

import queue


__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

__BASE_TYPES__ = (str, float, int, bool)


_retryable_statuses = [400, 408, 429]

def _headers(config):
    base_headers = _base_headers(config)
    base_headers.update({'Content-Type': "application/json"})
    return base_headers

def check_uwsgi():
    if 'uwsgi' in sys.modules:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        import uwsgi
        if not hasattr(uwsgi, 'opt'):
            # means that we are not running under uwsgi
            return

        if uwsgi.opt.get('enable-threads'):
            return
        if uwsgi.opt.get('threads') is not None and int(uwsgi.opt.get('threads')) > 1:
            return
        log.error("The LaunchDarkly client requires the 'enable-threads' or 'threads' option be passed to uWSGI. "
                    'To learn more, see https://docs.launchdarkly.com/sdk/server-side/python#configuring-uwsgi')


class Event:
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


def throw_if_unsuccessful_response(resp):
    if resp.status >= 400:
        raise UnsuccessfulResponseException(resp.status)


def is_http_error_recoverable(status):
    if status >= 400 and status < 500:
        return status in _retryable_statuses # all other 4xx besides these are unrecoverable
    return True  # all other errors are recoverable


def http_error_description(status):
    return "HTTP error %d%s" % (status, " (invalid SDK key)" if (status == 401 or status == 403) else "")


def http_error_message(status, context, retryable_message = "will retry"):
    return "Received %s for %s - %s" % (
        http_error_description(status),
        context,
        retryable_message if is_http_error_recoverable(status) else "giving up permanently"
        )


def check_if_error_is_recoverable_and_log(error_context, status_code, error_desc, recoverable_message):
    if status_code and (error_desc is None):
        error_desc = http_error_description(status_code)
    if status_code and not is_http_error_recoverable(status_code):
        log.error("Error %s (giving up permanently): %s" % (error_context, error_desc))
        return False
    log.warning("Error %s (%s): %s" % (error_context, recoverable_message, error_desc))
    return True


def stringify_attrs(attrdict, attrs):
    if attrdict is None:
        return None
    newdict = None
    for attr in attrs:
        val = attrdict.get(attr)
        if val is not None and not isinstance(val, str):
            if newdict is None:
                newdict = attrdict.copy()
            newdict[attr] = str(val)
    return attrdict if newdict is None else newdict
