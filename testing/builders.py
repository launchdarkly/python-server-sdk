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
    
    def offVariation(self, value: Optional[int]) -> FlagBuilder:
        return self._set('offVariation', value)

    def target(self, variation: int, *keys: str) -> FlagBuilder:
        self.__data['targets'].append({'variation': variation, 'values': list(keys)})
        return self
