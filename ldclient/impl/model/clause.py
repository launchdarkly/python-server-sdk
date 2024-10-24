from re import Pattern
from typing import Any, List, Optional

from semver import VersionInfo

from ldclient.impl.model.attribute_ref import (
    AttributeRef, req_attr_ref_with_opt_context_kind)
from ldclient.impl.model.entity import *
from ldclient.impl.model.value_parsing import (parse_regex, parse_semver,
                                               parse_time)


class ClausePreprocessedValue:
    __slots__ = ['_as_time', '_as_regex', '_as_semver']

    def __init__(self, as_time: Optional[float] = None, as_regex: Optional[Pattern] = None, as_semver: Optional[VersionInfo] = None):
        self._as_time = as_time
        self._as_regex = as_regex
        self._as_semver = as_semver

    @property
    def as_time(self) -> Optional[float]:
        return self._as_time

    @property
    def as_regex(self) -> Optional[Pattern]:
        return self._as_regex

    @property
    def as_semver(self) -> Optional[VersionInfo]:
        return self._as_semver


def _preprocess_clause_values(op: str, values: List[Any]) -> Optional[List[ClausePreprocessedValue]]:
    if op == 'matches':
        return list(ClausePreprocessedValue(as_regex=parse_regex(value)) for value in values)
    if op == 'before' or op == 'after':
        return list(ClausePreprocessedValue(as_time=parse_time(value)) for value in values)
    if op == 'semVerEqual' or op == 'semVerGreaterThan' or op == 'semVerLessThan':
        return list(ClausePreprocessedValue(as_semver=parse_semver(value)) for value in values)
    return None


class Clause:
    __slots__ = ['_context_kind', '_attribute', '_op', '_negate', '_values', '_values_preprocessed']

    def __init__(self, data: dict):
        self._context_kind = opt_str(data, 'contextKind')
        self._attribute = req_attr_ref_with_opt_context_kind(req_str(data, 'attribute'), self._context_kind)
        self._negate = opt_bool(data, 'negate')
        self._op = req_str(data, 'op')
        self._values = req_list(data, 'values')
        self._values_preprocessed = _preprocess_clause_values(self._op, self._values)

    @property
    def attribute(self) -> AttributeRef:
        return self._attribute

    @property
    def context_kind(self) -> Optional[str]:
        return self._context_kind

    @property
    def negate(self) -> bool:
        return self._negate

    @property
    def op(self) -> str:
        return self._op

    @property
    def values(self) -> List[Any]:
        return self._values

    @property
    def values_preprocessed(self) -> Optional[List[ClausePreprocessedValue]]:
        return self._values_preprocessed
