from typing import Any, Dict, List, Optional

from ldclient.context import Context
from ldclient.impl.model import AttributeRef


class EventContextFormatter:
    IGNORE_ATTRS = frozenset(['key', 'custom', 'anonymous'])
    ALLOWED_TOP_LEVEL_ATTRS = frozenset(['key', 'secondary', 'ip', 'country', 'email', 'firstName', 'lastName', 'avatar', 'name', 'anonymous', 'custom'])

    def __init__(self, all_attributes_private: bool, private_attributes: List[str]):
        self._all_attributes_private = all_attributes_private
        self._private_attributes = []  # type: List[AttributeRef]
        for p in private_attributes:
            ar = AttributeRef.from_path(p)
            if ar.valid:
                self._private_attributes.append(ar)

    def format_context(self, context: Context) -> Dict:
        """
        Formats a context for use in an analytic event, performing any
        necessary attribute redaction.
        """
        return self._format_context(context, False)

    def format_context_redact_anonymous(self, context: Context) -> Dict:
        """
        Formats a context for use in an analytic event, performing any
        necessary attribute redaction.

        If a context is anonoymous, all attributes will be redacted except for
        key, kind, and anonoymous.
        """
        return self._format_context(context, True)

    def _format_context(self, context: Context, redact_anonymous: bool) -> Dict:
        if context.multiple:
            out = {'kind': 'multi'}  # type: Dict[str, Any]
            for i in range(context.individual_context_count):
                c = context.get_individual_context(i)
                if c is not None:
                    out[c.kind] = self._format_context_single(c, False, redact_anonymous)
            return out
        else:
            return self._format_context_single(context, True, redact_anonymous)

    def _format_context_single(self, context: Context, include_kind: bool, redact_anonymous: bool) -> Dict:
        out = {'key': context.key}  # type: Dict[str, Any]
        if include_kind:
            out['kind'] = context.kind

        if context.anonymous:
            out['anonymous'] = True

        redacted = []  # type: List[str]
        all_private = self._private_attributes
        for p in context.private_attributes:
            if all_private is self._private_attributes:
                all_private = all_private.copy()
            ar = AttributeRef.from_path(p)
            if ar.valid:
                all_private.append(ar)

        if context.name is not None and not self._check_whole_attr_private('name', all_private, redacted, context.anonymous and redact_anonymous):
            out['name'] = context.name

        for attr in context.custom_attributes:
            if not self._check_whole_attr_private(attr, all_private, redacted, context.anonymous and redact_anonymous):
                value = context.get(attr)
                out[attr] = self._redact_json_value(None, attr, value, all_private, redacted)

        if len(redacted) != 0:
            out['_meta'] = {'redactedAttributes': redacted}

        return out

    def _check_whole_attr_private(self, attr: str, all_private: List[AttributeRef], redacted: List[str], redact_all: bool) -> bool:
        if self._all_attributes_private or redact_all:
            redacted.append(attr)
            return True
        for p in all_private:
            if p.depth == 1 and p[0] == attr:
                redacted.append(attr)
                return True
        return False

    def _redact_json_value(self, parent_path: Optional[List[str]], name: str, value: Any, all_private: List[AttributeRef], redacted: List[str]) -> Any:
        if not isinstance(value, dict) or len(value) == 0:
            return value
        ret = {}
        current_path = parent_path.copy() if parent_path else []
        current_path.append(name)
        for k, v in value.items():
            was_redacted = False
            for p in all_private:
                if p.depth != len(current_path) + 1:
                    continue
                if p[len(current_path)] != k:
                    continue
                match = True
                for i, component in enumerate(current_path):
                    if p[i] != component:
                        match = False
                        break
                if match:
                    redacted.append(p.path)
                    was_redacted = True
                    break
            if not was_redacted:
                ret[k] = self._redact_json_value(current_path, k, v, all_private, redacted)
        return ret
