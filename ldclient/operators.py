import re
from numbers import Number
from collections import defaultdict

import six


def _string_operator(u, c, fn):
    if isinstance(u, six.string_types):
        if isinstance(c, six.string_types):
            return fn(u, c)
    return False


def _numeric_operator(u, c, fn):
    if isinstance(u, Number):
        if isinstance(c, Number):
            return fn(u, c)
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
    return False


def _after(u, c):
    return False

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
