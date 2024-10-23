from collections import defaultdict
from numbers import Number
from typing import Any, Callable, Optional

from semver import VersionInfo

from ldclient.impl.model.clause import ClausePreprocessedValue
from ldclient.impl.model.value_parsing import (is_number, parse_semver,
                                               parse_time)


def _string_operator(context_value: Any, clause_value: Any, fn: Callable[[str, str], bool]) -> bool:
    return isinstance(context_value, str) and isinstance(clause_value, str) and fn(context_value, clause_value)


def _numeric_operator(context_value: Any, clause_value: Any, fn: Callable[[float, float], bool]) -> bool:
    return is_number(context_value) and is_number(clause_value) and fn(float(context_value), float(clause_value))


def _time_operator(clause_preprocessed: Optional[ClausePreprocessedValue], context_value: Any, fn: Callable[[float, float], bool]) -> bool:
    clause_time = None if clause_preprocessed is None else clause_preprocessed.as_time
    if clause_time is None:
        return False
    context_time = parse_time(context_value)
    return context_time is not None and fn(context_time, clause_time)


def _semver_operator(clause_preprocessed: Optional[ClausePreprocessedValue], context_value: Any, fn: Callable[[VersionInfo, VersionInfo], bool]) -> bool:
    clause_ver = None if clause_preprocessed is None else clause_preprocessed.as_semver
    if clause_ver is None:
        return False
    context_ver = parse_semver(context_value)
    return context_ver is not None and fn(context_ver, clause_ver)


def _in(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]) -> bool:
    return context_value == clause_value


def _starts_with(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]) -> bool:
    return _string_operator(context_value, clause_value, lambda a, b: a.startswith(b))


def _ends_with(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _string_operator(context_value, clause_value, lambda a, b: a.endswith(b))


def _contains(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _string_operator(context_value, clause_value, lambda a, b: b in a)


def _matches(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    clause_regex = None if clause_preprocessed is None else clause_preprocessed.as_regex
    if clause_regex is None:
        return False
    return isinstance(context_value, str) and clause_regex.search(context_value) is not None


def _less_than(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _numeric_operator(context_value, clause_value, lambda a, b: a < b)


def _less_than_or_equal(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _numeric_operator(context_value, clause_value, lambda a, b: a <= b)


def _greater_than(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _numeric_operator(context_value, clause_value, lambda a, b: a > b)


def _greater_than_or_equal(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _numeric_operator(context_value, clause_value, lambda a, b: a >= b)


def _before(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _time_operator(clause_preprocessed, context_value, lambda a, b: a < b)


def _after(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _time_operator(clause_preprocessed, context_value, lambda a, b: a > b)


def _semver_equal(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _semver_operator(clause_preprocessed, context_value, lambda a, b: a.compare(b) == 0)


def _semver_less_than(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _semver_operator(clause_preprocessed, context_value, lambda a, b: a.compare(b) < 0)


def _semver_greater_than(context_value: Any, clause_value: Any, clause_preprocessed: Optional[ClausePreprocessedValue]):
    return _semver_operator(clause_preprocessed, context_value, lambda a, b: a.compare(b) > 0)


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
    "semVerGreaterThan": _semver_greater_than,
}


def __default_factory():
    return lambda _l, _r, _p: False


ops = defaultdict(__default_factory, ops)
