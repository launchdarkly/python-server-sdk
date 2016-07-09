import logging
import re
import sys
from collections import defaultdict
from numbers import Number

import six
import strict_rfc3339

log = logging.getLogger(sys.modules[__name__].__name__)


def _string_operator(u, c, fn):
    if isinstance(u, six.string_types):
        if isinstance(c, six.string_types):
            return fn(u, c)
    return False


def _numeric_operator(u, c, fn):
    # bool is a subtype of int, and we don't want to try and compare it as a number.
    if isinstance(input, bool):
        log.warn("Got unexpected bool type when attempting to parse time")
        return None

    if isinstance(u, Number):
        if isinstance(c, Number):
            return fn(u, c)
    return False


def _parse_time(input):
    """
    :param input: Either a number as milliseconds since Unix Epoch, or a string as a valid RFC3339 timestamp
    :return: milliseconds since Unix epoch, or None if input was invalid.
    """

    # bool is a subtype of int, and we don't want to try and compare it as a time.
    if isinstance(input, bool):
        log.warn("Got unexpected bool type when attempting to parse time")
        return None

    if isinstance(input, Number):
        return float(input)

    if isinstance(input, six.string_types):
        try:
            timestamp = strict_rfc3339.rfc3339_to_timestamp(input)
            return timestamp * 1000.0
        except Exception as e:
            log.warn("Couldn't parse timestamp:" + str(input) + " with error: " + str(e))
            return None

    log.warn("Got unexpected type: " + type(input) + " with value: " + str(input) + " when attempting to parse time")
    return None


def _time_operator(u, c, fn):
    u_time = _parse_time(u)
    if u_time is not None:
        c_time = _parse_time(c)
        if c_time is not None:
            return fn(u_time, c_time)
    return False


def _in(u, c):
    if u == c:
        return True
    return False


def _starts_with(u, c):
    return _string_operator(u, c, lambda u, c: u.startswith(c))


def _ends_with(u, c):
    return _string_operator(u, c, lambda u, c: u.endswith(c))


def _contains(u, c):
    return _string_operator(u, c, lambda u, c: c in u)


def _matches(u, c):
    return _string_operator(u, c, lambda u, c: re.match(c, u))


def _less_than(u, c):
    return _numeric_operator(u, c, lambda u, c: u < c)


def _less_than_or_equal(u, c):
    return _numeric_operator(u, c, lambda u, c: u <= c)


def _greater_than(u, c):
    return _numeric_operator(u, c, lambda u, c: u > c)


def _greater_than_or_equal(u, c):
    return _numeric_operator(u, c, lambda u, c: u >= c)


def _before(u, c):
    return _time_operator(u, c, lambda u, c: u < c)


def _after(u, c):
    return _time_operator(u, c, lambda u, c: u > c)


ops = {
    "in": _in,
    "endsWith": _ends_with,
    "startsWith": _starts_with,
    "matches": _matches,
    "contains": _contains,
    "lessThan": _less_than,
    "lessThanOrEqual": _less_than_or_equal,
    "greaterThan": _greater_than,
    "greaterThanOrEqual": _greater_than_or_equal,
    "before": _before,
    "after": _after
}

ops = defaultdict(lambda: False, ops)
