import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig
from ldclient.hook import EvaluationSeriesContext, Hook, Metadata
from ldclient.testing.http_util import JsonResponse, start_server
from ldclient.testing.stub_util import make_put_event, stream_content
from ldclient.testing.test_ldclient_end_to_end import (
    always_true_flag,
    sdk_key,
    user
)


class CapturingHook(Hook):
    def __init__(self):
        self.contexts = []

    @property
    def metadata(self) -> Metadata:
        return Metadata(name='capturing-hook')

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        self.contexts.append(series_context)
        return data

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail) -> dict:
        return data


@pytest.mark.asyncio
async def test_hooks_receive_environment_id_in_streaming_mode():
    hook = CapturingHook()
    with start_server() as stream_server:
        with stream_content(make_put_event([always_true_flag]), headers={'X-LD-EnvID': 'env-abc-123'}) as stream_handler:
            stream_server.for_path('/all', stream_handler)
            config = AsyncConfig(sdk_key=sdk_key, stream_uri=stream_server.uri, send_events=False, hooks=[hook])

            async with AsyncLDClient(config) as client:
                assert await client.is_initialized()
                await client.variation(always_true_flag['key'], user, False)

                assert len(hook.contexts) == 1
                assert hook.contexts[0].environment_id == 'env-abc-123'


@pytest.mark.asyncio
async def test_hooks_receive_environment_id_in_polling_mode():
    hook = CapturingHook()
    with start_server() as poll_server:
        poll_server.for_path('/sdk/latest-all', JsonResponse({'flags': {always_true_flag['key']: always_true_flag}, 'segments': {}}, headers={'X-LD-EnvID': 'env-abc-123'}))
        config = AsyncConfig(sdk_key=sdk_key, base_uri=poll_server.uri, stream=False, send_events=False, hooks=[hook])

        async with AsyncLDClient(config) as client:
            assert await client.is_initialized()
            await client.variation(always_true_flag['key'], user, False)

            assert hook.contexts[0].environment_id == 'env-abc-123'
