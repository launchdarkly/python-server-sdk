from typing import Any, List, Optional, Set

from ldclient.impl.model.attribute_ref import (
    AttributeRef, opt_attr_ref_with_opt_context_kind)
from ldclient.impl.model.clause import Clause
from ldclient.impl.model.entity import *


class SegmentTarget:
    __slots__ = ['_context_kind', '_values']

    def __init__(self, data: dict, logger=None):
        self._context_kind = opt_str(data, 'contextKind')
        self._values = set(req_str_list(data, 'values'))

    @property
    def context_kind(self) -> Optional[str]:
        return self._context_kind

    @property
    def values(self) -> Set[str]:
        return self._values


class SegmentRule:
    __slots__ = ['_bucket_by', '_clauses', '_rollout_context_kind', '_weight']

    def __init__(self, data: dict):
        self._clauses = list(Clause(item) for item in req_dict_list(data, 'clauses'))
        self._rollout_context_kind = opt_str(data, 'rolloutContextKind')
        self._bucket_by = opt_attr_ref_with_opt_context_kind(opt_str(data, 'bucketBy'), self._rollout_context_kind)
        self._weight = opt_int(data, 'weight')

    @property
    def bucket_by(self) -> Optional[AttributeRef]:
        return self._bucket_by

    @property
    def clauses(self) -> List[Clause]:
        return self._clauses

    @property
    def rollout_context_kind(self) -> Optional[str]:
        return self._rollout_context_kind

    @property
    def weight(self) -> Optional[int]:
        return self._weight


class Segment(ModelEntity):
    __slots__ = [
        '_data',
        '_key',
        '_version',
        '_deleted',
        '_included',
        '_excluded',
        '_included_contexts',
        '_excluded_contexts',
        '_rules',
        '_salt',
        '_unbounded',
        '_unbounded_context_kind',
        '_generation',
    ]

    def __init__(self, data: dict):
        super().__init__(data)
        # In the following logic, we're being somewhat lenient in terms of allowing most properties to
        # be absent even if they are really required in the schema. That's for backward compatibility
        # with test logic that constructed incomplete JSON, and also with the file data source which
        # previously allowed users to get away with leaving out a lot of properties in the JSON.
        self._key = req_str(data, 'key')
        self._version = req_int(data, 'version')
        self._deleted = opt_bool(data, 'deleted')
        if self._deleted:
            return
        self._included = set(opt_str_list(data, 'included'))
        self._excluded = set(opt_str_list(data, 'excluded'))
        self._included_contexts = list(SegmentTarget(item) for item in opt_dict_list(data, 'includedContexts'))
        self._excluded_contexts = list(SegmentTarget(item) for item in opt_dict_list(data, 'excludedContexts'))
        self._rules = list(SegmentRule(item) for item in opt_dict_list(data, 'rules'))
        self._salt = opt_str(data, 'salt') or ''
        self._unbounded = opt_bool(data, 'unbounded')
        self._unbounded_context_kind = opt_str(data, 'unboundedContextKind')
        self._generation = opt_int(data, 'generation')

    @property
    def key(self) -> str:
        return self._key

    @property
    def version(self) -> int:
        return self._version

    @property
    def deleted(self) -> bool:
        return self._deleted

    @property
    def included(self) -> Set[str]:
        return self._included

    @property
    def excluded(self) -> Set[str]:
        return self._excluded

    @property
    def included_contexts(self) -> List[SegmentTarget]:
        return self._included_contexts

    @property
    def excluded_contexts(self) -> List[SegmentTarget]:
        return self._excluded_contexts

    @property
    def rules(self) -> List[Any]:
        return self._rules

    @property
    def salt(self) -> str:
        return self._salt

    @property
    def unbounded(self) -> bool:
        return self._unbounded

    @property
    def unbounded_context_kind(self) -> Optional[str]:
        return self._unbounded_context_kind

    @property
    def generation(self) -> Optional[int]:
        return self._generation
