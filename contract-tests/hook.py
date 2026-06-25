import asyncio
from typing import Optional

import requests

from ldclient.evaluation import EvaluationDetail
from ldclient.hook import AsyncHook, EvaluationSeriesContext, Hook, Metadata


class PostingHook(Hook):
    def __init__(self, name: str, callback: str, data: dict, errors: dict):
        self.__name = name
        self.__callback = callback
        self.__data = data
        self.__errors = errors

    @property
    def metadata(self) -> Metadata:
        return Metadata(name=self.__name)

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        return self.__post("beforeEvaluation", series_context, data, None)

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        return self.__post("afterEvaluation", series_context, data, detail)

    def __post(self, stage: str, series_context: EvaluationSeriesContext, data: dict, detail: Optional[EvaluationDetail]) -> dict:
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

        return {**data, **self.__data.get(stage, {})}


class AsyncPostingHook(AsyncHook):
    """AsyncHook mirror of PostingHook for the async contract-test harness.

    The blocking requests.post is offloaded to a worker thread so it does not
    block the event loop.
    """

    def __init__(self, name: str, callback: str, data: dict, errors: dict):
        self.__name = name
        self.__callback = callback
        self.__data = data
        self.__errors = errors

    @property
    def metadata(self) -> Metadata:
        return Metadata(name=self.__name)

    async def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        return await self.__post("beforeEvaluation", series_context, data, None)

    async def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        return await self.__post("afterEvaluation", series_context, data, detail)

    async def __post(self, stage: str, series_context: EvaluationSeriesContext, data: dict, detail: Optional[EvaluationDetail]) -> dict:
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

        await asyncio.to_thread(requests.post, self.__callback, json=payload)

        return {**data, **self.__data.get(stage, {})}
