from __future__ import annotations
from typing import Any, List ,Optional

from ldclient.context import Context


class BaseBuilder:
    def __init__(self, data):
        self.data = data
    
    def _set(self, key: str, value: Any):
        self.data[key] = value
        return self
    
    def _append(self, key: str, item: dict):
        self.data[key].append(item)
        return self
    
    def _append_all(self, key: str, items: List[dict]):
        self.data[key].extend(items)
        return self

    def build(self):
        return self.data.copy()


class FlagBuilder(BaseBuilder):
    def __init__(self, key):
        super().__init__({
            'key': key,
            'version': 1,
            'on': False,
            'variations': [],
            'offVariation': None,
            'fallthrough': {},
            'prerequisites': [],
            'targets': [],
            'contextTargets': [],
            'rules': []
        })
    
    def key(self, key: str) -> FlagBuilder:
        return self._set('key', key)

    def version(self, version: int) -> FlagBuilder:
        return self._set('key', version)
    
    def on(self, on: bool) -> FlagBuilder:
        return self._set('on', on)

    def variations(self, *variations: Any) -> FlagBuilder:
        return self._set('variations', list(variations))
    
    def off_variation(self, value: Optional[int]) -> FlagBuilder:
        return self._set('offVariation', value)

    def fallthrough_variation(self, index: int) -> FlagBuilder:
        return self._set('fallthrough', {'variation': index})

    def target(self, variation: int, *keys: str) -> FlagBuilder:
        return self._append('targets', {'variation': variation, 'values': list(keys)})
    
    def context_target(self, context_kind: str, variation: int, *keys: str) -> FlagBuilder:
        return self._append('contextTargets',
            {'contextKind': context_kind, 'variation': variation, 'values': list(keys)})
    
    def rules(self, *rules: dict) -> FlagBuilder:
        return self._append_all('rules', list(rules))


class FlagRuleBuilder(BaseBuilder):
    def __init__(self):
        super().__init__({'clauses': []})
    
    def variation(self, variation: int) -> FlagRuleBuilder:
        return self._set('variation', variation)
    
    def clauses(self, *clauses: dict) -> FlagRuleBuilder:
        return self._append_all('clauses', list(clauses))


class SegmentBuilder(BaseBuilder):
    def __init__(self, key):
        super().__init__({
            'key': key,
            'version': 1,
            'included': [],
            'excluded': [],
            'rules': [],
            'unbounded': False
        })
    
    def key(self, key: str) -> SegmentBuilder:
        return self._set('key', key)

    def version(self, version: int) -> SegmentBuilder:
        return self._set('key', version)

    def salt(self, salt: str) -> SegmentBuilder:
        return self._set('salt', salt)

    def rules(self, *rules: dict) -> SegmentBuilder:
        return self._append_all('rules', list(rules))


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


def make_boolean_flag_matching_segment(segment: dict) -> dict:
    return make_boolean_flag_with_clauses(make_clause_matching_segment_key(segment['key']))

def make_boolean_flag_with_clauses(*clauses: dict) -> dict:
    return make_boolean_flag_with_rules(FlagRuleBuilder().clauses(*clauses).variation(0).build())

def make_boolean_flag_with_rules(*rules: dict) -> dict:
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

def negate_clause(clause: dict) -> dict:
    c = clause.copy()
    c['negate'] = not c.get('negate')
    return c
