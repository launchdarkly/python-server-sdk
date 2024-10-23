from __future__ import annotations

from typing import Any, List, Optional

from ldclient.context import Context
from ldclient.impl.model import *


class BaseBuilder:
    def __init__(self, data):
        self.data = data

    def _set(self, key: str, value: Any):
        self.data[key] = value
        return self

    def _append(self, key: str, item: dict):
        self.data[key].append(item)
        return self

    def _append_all(self, key: str, items: List[Any]):
        self.data[key].extend(items)
        return self

    def build(self):
        return self.data.copy()


class FlagBuilder(BaseBuilder):
    def __init__(self, key):
        super().__init__(
            {'key': key, 'version': 1, 'on': False, 'variations': [], 'offVariation': None, 'fallthrough': {}, 'prerequisites': [], 'targets': [], 'contextTargets': [], 'rules': [], 'salt': ''}
        )

    def build(self):
        return FeatureFlag(self.data.copy())

    def key(self, key: str) -> FlagBuilder:
        return self._set('key', key)

    def version(self, version: int) -> FlagBuilder:
        return self._set('version', version)

    def on(self, on: bool) -> FlagBuilder:
        return self._set('on', on)

    def variations(self, *variations: Any) -> FlagBuilder:
        return self._set('variations', list(variations))

    def off_variation(self, value: Optional[int]) -> FlagBuilder:
        return self._set('offVariation', value)

    def fallthrough_variation(self, index: int) -> FlagBuilder:
        return self._set('fallthrough', {'variation': index})

    def fallthrough_rollout(self, rollout: dict) -> FlagBuilder:
        return self._set('fallthrough', {'rollout': rollout})

    def prerequisite(self, key: str, variation: int) -> FlagBuilder:
        return self._append('prerequisites', {'key': key, 'variation': variation})

    def target(self, variation: int, *keys: str) -> FlagBuilder:
        return self._append('targets', {'variation': variation, 'values': list(keys)})

    def context_target(self, context_kind: str, variation: int, *keys: str) -> FlagBuilder:
        return self._append('contextTargets', {'contextKind': context_kind, 'variation': variation, 'values': list(keys)})

    def rules(self, *rules: dict) -> FlagBuilder:
        return self._append_all('rules', list(rules))

    def salt(self, value: str) -> FlagBuilder:
        return self._set('salt', value)

    def track_events(self, value: bool) -> FlagBuilder:
        return self._set('trackEvents', value)

    def track_events_fallthrough(self, value: bool) -> FlagBuilder:
        return self._set('trackEventsFallthrough', value)

    def debug_events_until_date(self, value: Optional[int]) -> FlagBuilder:
        return self._set('debugEventsUntilDate', value)

    def exclude_from_summaries(self, value: bool) -> FlagBuilder:
        return self._set('excludeFromSummaries', value)

    def sampling_ratio(self, value: int) -> FlagBuilder:
        return self._set('samplingRatio', value)

    def migrations(self, value: MigrationSettings) -> FlagBuilder:
        return self._set('migration', value)


class MigrationSettingsBuilder(BaseBuilder):
    def __init__(self):
        super().__init__({})

    def check_ratio(self, value: int) -> MigrationSettingsBuilder:
        return self._set('checkRatio', value)


class FlagRuleBuilder(BaseBuilder):
    def __init__(self):
        super().__init__({'clauses': []})

    def clauses(self, *clauses: dict) -> FlagRuleBuilder:
        return self._append_all('clauses', list(clauses))

    def id(self, value: str) -> FlagRuleBuilder:
        return self._set('id', value)

    def rollout(self, rollout: Optional[dict]) -> FlagRuleBuilder:
        return self._set('rollout', rollout)

    def track_events(self, value: bool) -> FlagRuleBuilder:
        return self._set('trackEvents', value)

    def variation(self, variation: int) -> FlagRuleBuilder:
        return self._set('variation', variation)


class SegmentBuilder(BaseBuilder):
    def __init__(self, key):
        super().__init__({'key': key, 'version': 1, 'included': [], 'excluded': [], 'includedContexts': [], 'excludedContexts': [], 'rules': [], 'unbounded': False, 'salt': ''})

    def build(self):
        return Segment(self.data.copy())

    def key(self, key: str) -> SegmentBuilder:
        return self._set('key', key)

    def version(self, version: int) -> SegmentBuilder:
        return self._set('version', version)

    def excluded(self, *keys: str) -> SegmentBuilder:
        return self._append_all('excluded', list(keys))

    def excluded_contexts(self, context_kind: str, *keys: str) -> SegmentBuilder:
        return self._append('excludedContexts', {'contextKind': context_kind, 'values': list(keys)})

    def included(self, *keys: str) -> SegmentBuilder:
        return self._append_all('included', list(keys))

    def included_contexts(self, context_kind: str, *keys: str) -> SegmentBuilder:
        return self._append('includedContexts', {'contextKind': context_kind, 'values': list(keys)})

    def salt(self, salt: str) -> SegmentBuilder:
        return self._set('salt', salt)

    def rules(self, *rules: dict) -> SegmentBuilder:
        return self._append_all('rules', list(rules))

    def unbounded(self, value: bool) -> SegmentBuilder:
        return self._set('unbounded', value)

    def unbounded_context_kind(self, value: Optional[str]) -> SegmentBuilder:
        return self._set('unboundedContextKind', value)

    def generation(self, value: Optional[int]) -> SegmentBuilder:
        return self._set('generation', value)


class SegmentRuleBuilder(BaseBuilder):
    def __init__(self):
        super().__init__({'clauses': []})

    def bucket_by(self, value: Optional[str]) -> SegmentRuleBuilder:
        return self._set('bucketBy', value)

    def clauses(self, *clauses: dict) -> SegmentRuleBuilder:
        return self._append_all('clauses', list(clauses))

    def rollout_context_kind(self, value: Optional[str]) -> SegmentRuleBuilder:
        return self._set('rolloutContextKind', value)

    def weight(self, value: Optional[int]) -> SegmentRuleBuilder:
        return self._set('weight', value)


def build_off_flag_with_value(key: str, value: Any) -> FlagBuilder:
    return FlagBuilder(key).version(100).on(False).variations(value).off_variation(0)


def make_boolean_flag_matching_segment(segment: Segment) -> FeatureFlag:
    return make_boolean_flag_with_clauses(make_clause_matching_segment_key(segment.key))


def make_boolean_flag_with_clauses(*clauses: dict) -> FeatureFlag:
    return make_boolean_flag_with_rules(FlagRuleBuilder().clauses(*clauses).variation(0).build())


def make_boolean_flag_with_rules(*rules: dict) -> FeatureFlag:
    return FlagBuilder('flagkey').on(True).variations(True, False).fallthrough_variation(1).rules(*rules).build()


def make_clause(context_kind: Optional[str], attr: str, op: str, *values: Any) -> dict:
    ret = {'attribute': attr, 'op': op, 'values': list(values)}
    if context_kind is not None:
        ret['contextKind'] = context_kind
    return ret


def make_clause_matching_context(context: Context) -> dict:
    return {'contextKind': context.kind, 'attribute': 'key', 'op': 'in', 'values': [context.key]}


def make_clause_matching_segment_key(*segment_keys: str) -> dict:
    return {'attribute': '', 'op': 'segmentMatch', 'values': list(segment_keys)}


def make_segment_rule_matching_context(context: Context) -> dict:
    return SegmentRuleBuilder().clauses(make_clause_matching_context(context)).build()


def negate_clause(clause: dict) -> dict:
    c = clause.copy()
    c['negate'] = not c.get('negate')
    return c
