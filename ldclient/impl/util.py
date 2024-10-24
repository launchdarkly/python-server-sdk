import logging
import re
import sys
import time
from datetime import timedelta
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

from ldclient.impl.http import _base_headers


def current_time_millis() -> int:
    return int(time.time() * 1000)


def timedelta_millis(delta: timedelta) -> float:
    return delta / timedelta(milliseconds=1)


log = logging.getLogger('ldclient.util')  # historical logger name


__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email", "firstName", "lastName", "avatar", "name", "anonymous"]

__BASE_TYPES__ = (str, float, int, bool)


_retryable_statuses = [400, 408, 429]


def validate_application_info(application: dict, logger: logging.Logger) -> dict:
    return {
        "id": validate_application_value(application.get("id", ""), "id", logger),
        "version": validate_application_value(application.get("version", ""), "version", logger),
    }


def validate_application_value(value: Any, name: str, logger: logging.Logger) -> str:
    if not isinstance(value, str):
        return ""

    if len(value) > 64:
        logger.warning('Value of application[%s] was longer than 64 characters and was discarded' % name)
        return ""

    if re.search(r"[^a-zA-Z0-9._-]", value):
        logger.warning('Value of application[%s] contained invalid characters and was discarded' % name)
        return ""

    return value


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
        log.error(
            "The LaunchDarkly client requires the 'enable-threads' or 'threads' option be passed to uWSGI. "
            'To learn more, read https://docs.launchdarkly.com/sdk/server-side/python#configuring-uwsgi'
        )


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
        return status in _retryable_statuses  # all other 4xx besides these are unrecoverable
    return True  # all other errors are recoverable


def http_error_description(status):
    return "HTTP error %d%s" % (status, " (invalid SDK key)" if (status == 401 or status == 403) else "")


def http_error_message(status, context, retryable_message="will retry"):
    return "Received %s for %s - %s" % (http_error_description(status), context, retryable_message if is_http_error_recoverable(status) else "giving up permanently")


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


def redact_password(url: str) -> str:
    """
    Replace any embedded password in the provided URL with 'xxxx'. This is
    useful for ensuring sensitive information included in a URL isn't logged.
    """
    parts = urlparse(url)
    if parts.password is None:
        return url

    updated = parts.netloc.replace(parts.password, "xxxx")
    parts = parts._replace(netloc=updated)

    return urlunparse(parts)


class Result:
    """
    A Result is used to reflect the outcome of any operation.

    Results can either be considered a success or a failure.

    In the event of success, the Result will contain an option, nullable value
    to hold any success value back to the calling function.

    If the operation fails, the Result will contain an error describing the
    value.
    """

    def __init__(self, value: Optional[Any], error: Optional[str], exception: Optional[Exception]):
        """
        This constructor should be considered private. Consumers of this class
        should use one of the two factory methods provided. Direct
        instantiation should follow the below expectations:

        - Successful operations have contain a value, but *MUST NOT* contain an
          error or an exception value.
        - Failed operations *MUST* contain an error string, and may optionally
          include an exception.

        :param value: A result value when the operation was a success
        :param error: An error describing the cause of the failure
        :param exception: An optional exception if the failure resulted from an
            exception being raised
        """
        self.__value = value
        self.__error = error
        self.__exception = exception

    @staticmethod
    def success(value: Any) -> 'Result':
        """
        Construct a successful result containing the provided value.

        :param value: A result value when the operation was a success
        :return: The successful result instance
        """
        return Result(value, None, None)

    @staticmethod
    def fail(error: str, exception: Optional[Exception] = None) -> 'Result':
        """
        Construct a failed result containing an error description and optional
        exception.

        :param error: An error describing the cause of the failure
        :param exception: An optional exception if the failure resulted from an
            exception being raised
        :return: The successful result instance
        """
        return Result(None, error, exception)

    def is_success(self) -> bool:
        """
        Determine whether this result represents success or failure by checking
        for the presence of an error.
        """
        return self.__error is None

    @property
    def value(self) -> Optional[Any]:
        return self.__value

    @property
    def error(self) -> Optional[str]:
        return self.__error

    @property
    def exception(self) -> Optional[Exception]:
        return self.__exception
