"""
Implementation details of feature flag evaluation.
"""
# currently excluded from documentation - see docs/README.md

import logging
import re
from semver import VersionInfo
import sys
from datetime import tzinfo, timedelta, datetime
from collections import defaultdict
from numbers import Number

import pyrfc3339

log = logging.getLogger(sys.modules[__name__].__name__)


def _string_operator(u, c, fn):
    return fn(u, c) if isinstance(u, str) and isinstance(c, str) else False

def _numeric_operator(u, c, fn):
    # bool is a subtype of int, and we don't want to try and compare it as a number.
    if isinstance(input, bool):
        log.warning("Got unexpected bool type when attempting to parse time")
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
        log.warning("Got unexpected bool type when attempting to parse time")
        return None

    if isinstance(input, Number):
        return float(input)

    if isinstance(input, str):
        try:
            parsed_time = pyrfc3339.parse(input)
            timestamp = (parsed_time - epoch).total_seconds()
            return timestamp * 1000.0
        except Exception as e:
            log.warning("Couldn't parse timestamp:" + str(input) + " with message: " + str(e))
            return None

    log.warning("Got unexpected type: " + type(input) + " with value: " + str(input) + " when attempting to parse time")
    return None

def _time_operator(u, c, fn):
    u_time = _parse_time(u)
    if u_time is not None:
        c_time = _parse_time(c)
        if c_time is not None:
            return fn(u_time, c_time)
    return False

def _parse_semver(input):
    try:
        VersionInfo.parse(input)
        return input
    except ValueError as e:
        try:
            input = _add_zero_version_component(input)
            VersionInfo.parse(input)
            return input
        except ValueError as e:
            try:
                input = _add_zero_version_component(input)
                VersionInfo.parse(input)
                return input
            except ValueError as e:
                return None

def _add_zero_version_component(input):
    m = re.search("^([0-9.]*)(.*)", input)
    if m is None:
        return input + ".0"
    return m.group(1) + ".0" + m.group(2)

def _semver_operator(u, c, fn):
    u_ver = _parse_semver(u)
    c_ver = _parse_semver(c)
    if u_ver is not None and c_ver is not None:
        return fn(u_ver, c_ver)
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
    return _string_operator(u, c, lambda u, c: re.search(c, u) is not None)


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


def _semver_equal(u, c):
    return _semver_operator(u, c, lambda u, c: VersionInfo.parse(u).compare(c) == 0)


def _semver_less_than(u, c):
    return _semver_operator(u, c, lambda u, c: VersionInfo.parse(u).compare(c) < 0)


def _semver_greater_than(u, c):
    return _semver_operator(u, c, lambda u, c: VersionInfo.parse(u).compare(c) > 0)


_ZERO = timedelta(0)
_HOUR = timedelta(hours=1)

# A UTC class.

class _UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return _ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _ZERO

epoch = datetime.utcfromtimestamp(0).replace(tzinfo=_UTC())

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
    "after": _after,
    "semVerEqual": _semver_equal,
    "semVerLessThan": _semver_less_than,
    "semVerGreaterThan": _semver_greater_than
}

ops = defaultdict(lambda: lambda l, r: False, ops)
