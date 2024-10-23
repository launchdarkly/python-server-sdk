from __future__ import annotations

import re
from typing import List, Optional


def req_attr_ref_with_opt_context_kind(attr_ref_str: str, context_kind: Optional[str]) -> AttributeRef:
    if context_kind is None or context_kind == '':
        return AttributeRef.from_literal(attr_ref_str)
    return AttributeRef.from_path(attr_ref_str)


def opt_attr_ref_with_opt_context_kind(attr_ref_str: Optional[str], context_kind: Optional[str]) -> Optional[AttributeRef]:
    if attr_ref_str is None or attr_ref_str == '':
        return None
    return req_attr_ref_with_opt_context_kind(attr_ref_str, context_kind)


_INVALID_ATTR_ESCAPE_REGEX = re.compile('(~[^01]|~$)')


class AttributeRef:
    __slots__ = ['_raw', '_single_component', '_components', '_error']

    _ERR_EMPTY = 'attribute reference cannot be empty'

    def __init__(self, raw: str, single_component: Optional[str], components: Optional[List[str]], error: Optional[str]):
        self._raw = raw
        self._single_component = single_component
        self._components = components
        self._error = error

    @property
    def valid(self) -> bool:
        return self._error is None

    @property
    def error(self) -> Optional[str]:
        return self._error

    @property
    def path(self) -> str:
        return self._raw

    @property
    def depth(self) -> int:
        if self._error is not None:
            return 0
        if self._components is not None:
            return len(self._components)
        return 1

    def __getitem__(self, index) -> Optional[str]:
        if self._error is not None:
            return None
        if self._components is not None:
            return None if index < 0 or index >= len(self._components) else self._components[index]
        return self._single_component if index == 0 else None

    @staticmethod
    def from_path(path: str) -> AttributeRef:
        if path == '' or path == '/':
            return AttributeRef._from_error(AttributeRef._ERR_EMPTY)
        if path[0] != '/':
            return AttributeRef(path, path, None, None)
        components = path[1:].split('/')
        for i, c in enumerate(components):
            if c == '':
                return AttributeRef._from_error('attribute reference contained a double slash or a trailing slash')
            unescaped = AttributeRef._unescape(c)
            if unescaped is None:
                return AttributeRef._from_error('attribute reference contained an escape character (~) that was not followed by 0 or 1')
            components[i] = unescaped
        return AttributeRef(path, None, components, None)

    @staticmethod
    def from_literal(name: str) -> AttributeRef:
        if name == '':
            return AttributeRef._from_error(AttributeRef._ERR_EMPTY)
        return AttributeRef(AttributeRef._escape(name), name, None, None)

    @staticmethod
    def _from_error(error: str) -> AttributeRef:
        return AttributeRef('', None, None, error)

    @staticmethod
    def _unescape(s: str) -> Optional[str]:
        if _INVALID_ATTR_ESCAPE_REGEX.search(s):
            return None
        return s.replace("~1", "/").replace("~0", "~")

    @staticmethod
    def _escape(s: str) -> str:
        return s.replace("~", "~0").replace("/", "~1")
