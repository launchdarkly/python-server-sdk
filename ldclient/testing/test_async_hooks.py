"""
Tests for hook and plugin dispatch in AsyncLDClient.
"""
import asyncio
import contextvars
from typing import List

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.hook import EvaluationSeriesContext, Hook, Metadata
from ldclient.plugin import (
    AsyncPlugin,
    EnvironmentMetadata,
    Plugin,
    PluginMetadata
)
from ldclient.testing.mock_async_components import (
    MockAsyncFeatureStore,
    MockAsyncUpdateProcessor
)
from ldclient.versioned_data_kind import FEATURES


def _offline_config(**kwargs) -> AsyncConfig:
    return AsyncConfig("test-sdk-key", offline=True, **kwargs)


def _make_flag(key: str, value) -> dict:
    return {
        'key': key,
        'version': 1,
        'on': False,
        'offVariation': 0,
        'variations': [value],
        'targets': [],
        'contextTargets': [],
        'rules': [],
        'prerequisites': [],
        'salt': '',
    }


class RecordingHook(Hook):
    def __init__(self, name: str, log: List[str]):
        self._name = name
        self._log = log

    @property
    def metadata(self) -> Metadata:
        return Metadata(name=self._name)

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        self._log.append(f"{self._name}:before:{series_context.key}")
        return {**data, 'from_before': self._name}

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        self._log.append(f"{self._name}:after:{series_context.key}:{data.get('from_before')}:{detail.value}")
        return data


@pytest.mark.asyncio
async def test_hooks_from_config_run_in_order():
    log: List[str] = []
    config = _offline_config(hooks=[RecordingHook('first', log), RecordingHook('second', log)])

    async with AsyncLDClient(config) as client:
        await client.variation('flag', Context.create('user'), 'default')

    assert log == [
        'first:before:flag',
        'second:before:flag',
        'second:after:flag:second:default',
        'first:after:flag:first:default',
    ]


@pytest.mark.asyncio
async def test_hook_error_does_not_stop_evaluation():
    log: List[str] = []

    class FailingHook(Hook):
        @property
        def metadata(self) -> Metadata:
            return Metadata(name='failing')

        def before_evaluation(self, series_context, data):
            raise RuntimeError('before failed')

        def after_evaluation(self, series_context, data, detail):
            log.append(f"after:{data}")
            return data

    async with AsyncLDClient(_offline_config(hooks=[FailingHook()])) as client:
        result = await client.variation('flag', Context.create('user'), 'default')

    assert result == 'default'
    assert log == ['after:{}']


class _YieldingStore(MockAsyncFeatureStore):
    """Yields to the event loop on each read, so concurrent evaluations interleave."""

    def __init__(self, seen: List[str], var: contextvars.ContextVar):
        super().__init__()
        self._seen = seen
        self._var = var

    async def get(self, kind, key):
        self._seen.append(self._var.get())
        await asyncio.sleep(0)
        return await super().get(kind, key)


@pytest.mark.asyncio
async def test_context_variable_set_in_before_stage_is_kept_per_evaluation():
    # A tracing hook makes a span current in the before stage and restores the
    # previous context in the after stage. Concurrent evaluations must each see
    # only their own value, and the restore must not fail.
    current = contextvars.ContextVar('current', default='none')
    seen_in_store: List[str] = []
    seen_in_after: List[str] = []
    errors: List[BaseException] = []

    class ContextVarHook(Hook):
        @property
        def metadata(self) -> Metadata:
            return Metadata(name='context-var')

        def before_evaluation(self, series_context, data):
            return {'token': current.set(series_context.context.key)}

        def after_evaluation(self, series_context, data, detail):
            seen_in_after.append(f"{series_context.context.key}={current.get()}")
            try:
                current.reset(data['token'])
            except BaseException as e:
                errors.append(e)
            return data

    store = _YieldingStore(seen_in_store, current)
    await store.force_set(FEATURES, _make_flag('flag', 'on'))
    store._initialized = True
    config = AsyncConfig(
        "test-sdk-key",
        feature_store=store,
        update_processor_class=MockAsyncUpdateProcessor,
        send_events=False,
        hooks=[ContextVarHook()],
    )

    async with AsyncLDClient(config) as client:
        keys = ['a', 'b', 'c']
        results = await asyncio.gather(*(client.variation('flag', Context.create(k), 'off') for k in keys))

    assert results == ['on', 'on', 'on']
    assert errors == []
    assert sorted(seen_in_store) == keys
    assert sorted(seen_in_after) == ['a=a', 'b=b', 'c=c']
    assert current.get() == 'none'


@pytest.mark.asyncio
async def test_async_plugin_hooks_run_and_register_is_awaited():
    log: List[str] = []
    registered: List[AsyncLDClient] = []

    class RecordingPlugin(AsyncPlugin):
        @property
        def metadata(self) -> PluginMetadata:
            return PluginMetadata(name='recording-plugin')

        async def register(self, client, metadata: EnvironmentMetadata) -> None:
            await asyncio.sleep(0)
            registered.append(client)

        def get_hooks(self, metadata: EnvironmentMetadata) -> List[Hook]:
            return [RecordingHook('plugin-hook', log)]

    client = AsyncLDClient(_offline_config(plugins=[RecordingPlugin()]))
    async with client:
        await client.variation('flag', Context.create('user'), 'default')

    assert registered == [client]
    assert log == ['plugin-hook:before:flag', 'plugin-hook:after:flag:plugin-hook:default']


def test_config_rejects_non_hook():
    with pytest.raises(TypeError):
        _offline_config(hooks=[object()])  # type: ignore[list-item]


def test_config_rejects_sync_plugin():
    class SyncPlugin(Plugin):
        @property
        def metadata(self) -> PluginMetadata:
            return PluginMetadata(name='sync-plugin')

        def register(self, client, metadata):
            pass

        def get_hooks(self, metadata):
            return []

    with pytest.raises(TypeError):
        _offline_config(plugins=[SyncPlugin()])  # type: ignore[list-item]
