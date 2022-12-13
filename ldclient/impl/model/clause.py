from typing import Any, List, Optional

from ldclient.impl.model.entity import *

class Clause:
    __slots__ = ['_context_kind', '_attribute', '_op', '_values', '_negate']

    def __init__(self, data: dict):
        self._attribute = req_str(data, 'attribute')
        self._context_kind = opt_str(data, 'contextKind')
        self._negate = opt_bool(data, 'negate')
        self._op = req_str(data, 'op')
        self._values = req_list(data, 'values')

    @property
    def attribute(self) -> str:
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
