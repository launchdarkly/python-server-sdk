import re
from datetime import datetime, timedelta, timezone, tzinfo
from numbers import Number
from re import Pattern
from typing import Any, Optional

import pyrfc3339
from semver import VersionInfo

_ZERO = timedelta(0)


# A UTC class.
class _UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return _ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _ZERO


_epoch = datetime.fromtimestamp(0, timezone.utc)


def is_number(input: Any) -> bool:
    # bool is a subtype of int, and we don't want to try and treat it as a number.
    return isinstance(input, Number) and not isinstance(input, bool)


def parse_regex(input: Any) -> Optional[Pattern]:
    if isinstance(input, str):
        try:
            return re.compile(input)
        except Exception:
            return None
    return None


def parse_time(input: Any) -> Optional[float]:
    """
    :param input: Either a number as milliseconds since Unix Epoch, or a string as a valid RFC3339 timestamp
    :return: milliseconds since Unix epoch, or None if input was invalid.
    """

    if is_number(input):
        return float(input)

    if isinstance(input, str):
        try:
            parsed_time = pyrfc3339.parse(input)
            timestamp = (parsed_time - _epoch).total_seconds()
            return timestamp * 1000.0
        except Exception as e:
            return None

    return None


def parse_semver(input: Any) -> Optional[VersionInfo]:
    if not isinstance(input, str):
        return None
    try:
        return VersionInfo.parse(input)
    except TypeError:
        return None
    except ValueError as e:
        try:
            input = _add_zero_version_component(input)
            return VersionInfo.parse(input)
        except ValueError as e:
            try:
                input = _add_zero_version_component(input)
                return VersionInfo.parse(input)
            except ValueError as e:
                return None


def _add_zero_version_component(input):
    m = re.search("^([0-9.]*)(.*)", input)
    if m is None:
        return input + ".0"
    return m.group(1) + ".0" + m.group(2)
