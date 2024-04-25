from ldclient.hook import Hook, EvaluationSeriesContext
from ldclient.evaluation import EvaluationDetail

from typing import Any, Optional
import requests


class PostingHook(Hook):
    def __init__(self, name: str, callback: str, data: dict, errors: dict):
        self.__name = name
        self.__callback = callback
        self.__data = data
        self.__errors = errors

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> Any:
        return self.__post("beforeEvaluation", series_context, data, None)

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: Any, detail: EvaluationDetail) -> Any:
        return self.__post("afterEvaluation", series_context, data, detail)

    def __post(self, stage: str, series_context: EvaluationSeriesContext, data: Any, detail: Optional[EvaluationDetail]) -> Any:
        if stage in self.__errors:
            raise Exception(self.__errors[stage])

        payload = {
            'evaluationSeriesContext': {
                'flagKey': series_context.key,
                'context': series_context.context.to_dict(),
                'defaultValue': series_context.default_value,
                'method': series_context.method,
            },
            'evaluationSeriesData': data,
            'stage': stage,
        }

        if detail is not None:
            payload['evaluationDetail'] = {
                'value': detail.value,
                'variationIndex': detail.variation_index,
                'reason': detail.reason,
            }

        requests.post(self.__callback, json=payload)

        return {**(data or {}), **self.__data.get(stage, {})}
