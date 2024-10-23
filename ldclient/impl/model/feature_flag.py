from typing import Any, Dict, List, Optional, Set, Union

from ldclient.impl.model.clause import Clause
from ldclient.impl.model.entity import *
from ldclient.impl.model.variation_or_rollout import VariationOrRollout


class Prerequisite:
    __slots__ = ['_key', '_variation']

    def __init__(self, data: dict):
        self._key = req_str(data, 'key')
        self._variation = req_int(data, 'variation')

    @property
    def key(self) -> str:
        return self._key

    @property
    def variation(self) -> int:
        return self._variation


class Target:
    __slots__ = ['_context_kind', '_variation', '_values']

    def __init__(self, data: dict):
        self._context_kind = opt_str(data, 'contextKind')
        self._variation = req_int(data, 'variation')
        self._values = set(req_str_list(data, 'values'))

    @property
    def context_kind(self) -> Optional[str]:
        return self._context_kind

    @property
    def variation(self) -> int:
        return self._variation

    @property
    def values(self) -> Set[str]:
        return self._values


class FlagRule:
    __slots__ = ['_id', '_clauses', '_track_events', '_variation_or_rollout']

    def __init__(self, data: dict):
        self._id = opt_str(data, 'id')
        self._variation_or_rollout = VariationOrRollout(data)
        self._clauses = list(Clause(item) for item in req_dict_list(data, 'clauses'))
        self._track_events = opt_bool(data, 'trackEvents')

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def clauses(self) -> List[Clause]:
        return self._clauses

    @property
    def track_events(self) -> bool:
        return self._track_events

    @property
    def variation_or_rollout(self) -> VariationOrRollout:
        return self._variation_or_rollout


class MigrationSettings:
    __slots__ = ['_check_ratio']

    def __init__(self, data: Dict):
        self._check_ratio = opt_int(data, 'checkRatio')

    @property
    def check_ratio(self) -> Optional[int]:
        return self._check_ratio


class FeatureFlag(ModelEntity):
    __slots__ = [
        '_data',
        '_key',
        '_version',
        '_deleted',
        '_variations',
        '_on',
        '_off_variation',
        '_fallthrough',
        '_prerequisites',
        '_targets',
        '_context_targets',
        '_rules',
        '_salt',
        '_track_events',
        '_debug_events_until_date',
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
        self._variations = opt_list(data, 'variations')
        self._on = opt_bool(data, 'on')
        self._off_variation = opt_int(data, 'offVariation')
        self._fallthrough = VariationOrRollout(opt_dict(data, 'fallthrough'))
        self._prerequisites = list(Prerequisite(item) for item in opt_dict_list(data, 'prerequisites'))
        self._rules = list(FlagRule(item) for item in opt_dict_list(data, 'rules'))
        self._targets = list(Target(item) for item in opt_dict_list(data, 'targets'))
        self._context_targets = list(Target(item) for item in opt_dict_list(data, 'contextTargets'))
        self._salt = opt_str(data, 'salt') or ''
        self._track_events = opt_bool(data, 'trackEvents')
        self._track_events_fallthrough = opt_bool(data, 'trackEventsFallthrough')
        self._debug_events_until_date = opt_number(data, 'debugEventsUntilDate')

        self._migrations = None
        if 'migration' in data:
            self._migrations = MigrationSettings(opt_dict(data, 'migration') or {})

        self._exclude_from_summaries = opt_bool(data, 'excludeFromSummaries') or False
        self._sampling_ratio = opt_int(data, 'samplingRatio')

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
    def variations(self) -> List[Any]:
        return self._variations

    @property
    def on(self) -> bool:
        return self._on

    @property
    def off_variation(self) -> Optional[int]:
        return self._off_variation

    @property
    def fallthrough(self) -> VariationOrRollout:
        return self._fallthrough

    @property
    def prerequisites(self) -> List[Prerequisite]:
        return self._prerequisites

    @property
    def targets(self) -> List[Target]:
        return self._targets

    @property
    def context_targets(self) -> List[Target]:
        return self._context_targets

    @property
    def rules(self) -> List[FlagRule]:
        return self._rules

    @property
    def salt(self) -> str:
        return self._salt

    @property
    def track_events(self) -> bool:
        return self._track_events

    @property
    def track_events_fallthrough(self) -> bool:
        return self._track_events_fallthrough

    @property
    def debug_events_until_date(self) -> Optional[Union[int, float]]:
        return self._debug_events_until_date

    @property
    def migrations(self) -> Optional[MigrationSettings]:
        return self._migrations

    @property
    def exclude_from_summaries(self) -> bool:
        return self._exclude_from_summaries

    @property
    def sampling_ratio(self) -> Optional[int]:
        return self._sampling_ratio
