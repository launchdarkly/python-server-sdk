from typing import List, Optional

from ldclient.impl.model.attribute_ref import (
    AttributeRef, opt_attr_ref_with_opt_context_kind)
from ldclient.impl.model.entity import *


class WeightedVariation:
    __slots__ = ['_variation', '_weight', '_untracked']

    def __init__(self, data: dict):
        self._variation = req_int(data, 'variation')
        self._weight = req_int(data, 'weight')
        self._untracked = opt_bool(data, 'untracked')

    @property
    def variation(self) -> int:
        return self._variation

    @property
    def weight(self) -> int:
        return self._weight

    @property
    def untracked(self) -> int:
        return self._untracked


class Rollout:
    __slots__ = ['_bucket_by', '_context_kind', '_is_experiment', '_seed', '_variations']

    def __init__(self, data: dict):
        self._context_kind = opt_str(data, 'contextKind')
        self._bucket_by = opt_attr_ref_with_opt_context_kind(opt_str(data, 'bucketBy'), self._context_kind)
        self._is_experiment = opt_str(data, 'kind') == 'experiment'
        self._seed = opt_int(data, 'seed')
        self._variations = list(WeightedVariation(item) for item in req_dict_list(data, 'variations'))

    @property
    def bucket_by(self) -> Optional[AttributeRef]:
        return self._bucket_by

    @property
    def context_kind(self) -> Optional[str]:
        return self._context_kind

    @property
    def is_experiment(self) -> bool:
        return self._is_experiment

    @property
    def seed(self) -> Optional[int]:
        return self._seed

    @property
    def variations(self) -> List[WeightedVariation]:
        return self._variations


class VariationOrRollout:
    __slots__ = ['_variation', '_rollout']

    def __init__(self, data):
        data = {} if data is None else data
        self._variation = opt_int(data, 'variation')
        rollout = opt_dict(data, 'rollout')
        self._rollout = None if rollout is None else Rollout(rollout)

    @property
    def variation(self) -> Optional[int]:
        return self._variation

    @property
    def rollout(self) -> Optional[Rollout]:
        return self._rollout
