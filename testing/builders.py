from __future__ import annotations
from typing import Any, Optional


class FlagBuilder:
    def __init__(self, key):
        self.__data = {
            'key': key,
            'version': 1,
            'on': False,
            'variations': [],
            'offVariation': None,
            'fallthrough': {},
            'prerequisites': [],
            'targets': [],
            'rules': []
        }
    
    def build(self):
        return self.__data.copy()
    
    def _set(self, k: str, v: Any) -> FlagBuilder:
        self.__data[k] = v
        return self
    
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
        self.__data['targets'].append({'variation': variation, 'values': list(keys)})
        return self
    
    def rules(self, *rules: dict) -> FlagBuilder:
        for r in rules:
            self.__data['rules'].append(r)
        return self


class FlagRuleBuilder:
    def __init__(self):
        self.__data = {'clauses': []}
    
    def build(self) -> dict:
        return self.__data.copy()
    
    def variation(self, variation: int) -> FlagRuleBuilder:
        self.__data['variation'] = variation
        return self
    
    def clauses(self, *clauses: dict) -> FlagRuleBuilder:
        for c in clauses:
            self.__data['clauses'].append(c)
        return self


def make_boolean_flag_with_clauses(*clauses: dict) -> dict:
    return make_boolean_flag_with_rules(FlagRuleBuilder().clauses(*clauses).variation(0).build())

def make_boolean_flag_with_rules(*rules: dict) -> dict:
    return FlagBuilder('flagkey').on(True).variations(True, False).fallthrough_variation(1).rules(*rules).build()

def make_clause(context_kind: Optional[str], attr: str, op: str, *values: Any) -> dict:
    ret = {'attribute': attr, 'op': op, 'values': list(values)}
    if context_kind is not None:
        ret['contextKind'] = context_kind
    return ret

def negate_clause(clause: dict) -> dict:
    c = clause.copy()
    c['negate'] = not c.get('negate')
    return c
