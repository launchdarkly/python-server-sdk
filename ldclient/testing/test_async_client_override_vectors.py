"""
Runs the OVERRIDE specification test vectors through the async client. The vectors and the
checks are those of the sync runner: each vector sets up LaunchDarkly data, an override layer,
and an initialization state, and the test evaluates one flag through the full client stack.
"""
from typing import Any, Dict

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.context import Context
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.testing.mock_async_components import MockAsyncEventProcessor
from ldclient.testing.mock_components import (
    MockDataSourceBuilder,
    MockOverrideSource
)
from ldclient.testing.test_async_client_overrides import (
    AsyncHangingSynchronizer,
    AsyncStaticInitializer
)
from ldclient.testing.test_ldclient_override_vectors import (
    assert_reason,
    load_vectors,
    override_flags,
    vector_id
)


async def make_client(vector: Dict[str, Any]) -> AsyncLDClient:
    source = MockOverrideSource(flags=override_flags(vector['overrides']), segments=dict(vector['overrides'].get('segments', {})))
    ld_data = vector['launchDarklyData']
    if ld_data['initialized']:
        initializer = AsyncStaticInitializer(ld_data.get('flags', {}), ld_data.get('segments', {}))
        datasystem = AsyncDataSystemConfig(initializers=[MockDataSourceBuilder(initializer)], override_source=source.builder)
        start_wait = 5
    else:
        # With no sources at all, the client would consider cached data available rather than
        # applying its not-initialized handling. A synchronizer that never delivers avoids that.
        datasystem = AsyncDataSystemConfig(synchronizers=[MockDataSourceBuilder(AsyncHangingSynchronizer())], override_source=source.builder)
        start_wait = 0
    config = AsyncConfig('SDK_KEY', datasystem_config=datasystem, event_processor_class=lambda config: MockAsyncEventProcessor())
    client = AsyncLDClient(config)
    await client.start(start_wait=start_wait)
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", load_vectors(), ids=vector_id)
async def test_override_spec_vector(vector: Dict[str, Any]):
    client = await make_client(vector)
    try:
        assert await client.is_initialized() is vector['launchDarklyData']['initialized']
        evaluate = vector['evaluate']
        detail = await client.variation_detail(evaluate['flagKey'], Context.from_dict(evaluate['context']), evaluate['defaultValue'])

        expect = vector['expect']
        assert detail.value == expect['value'], "value"
        assert detail.variation_index == expect['variationIndex'], "variationIndex"
        assert_reason(expect['reason'], detail.reason)

        # summaryOverrideAffected is the marking the client hands to the event processor for this
        # evaluation. The event processor keys individual-event suppression and the summary
        # counter marker on that scalar, not on the reason.
        if 'summaryOverrideAffected' in expect:
            records = [e for e in client._event_processor.events if isinstance(e, EventInputEvaluation) and e.key == evaluate['flagKey']]
            assert len(records) == 1, "expected exactly one evaluation record for the flag"
            assert records[0].override_affected is expect['summaryOverrideAffected'], "summaryOverrideAffected"
    finally:
        await client.close()
