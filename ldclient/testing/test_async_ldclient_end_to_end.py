"""
End-to-end tests of the async client on the FDv2 data system, run against a
mock HTTP server.
"""

import time

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.client import Context
from ldclient.impl.datasource.async_polling import (
    AsyncPollingDataSourceBuilder
)
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingDataSourceBuilder
)
from ldclient.testing.http_util import (
    BasicResponse,
    CauseNetworkError,
    SequentialHandler,
    start_server
)
from ldclient.testing.test_ldclient_end_to_end import (
    always_true_flag,
    poll_response,
    sdk_key,
    stream_response
)

user = Context.from_dict({'key': 'userkey', 'kind': 'user'})


def make_config(uri, synchronizer):
    return AsyncConfig(
        sdk_key,
        base_uri=uri,
        stream_uri=uri,
        send_events=False,
        data_system=AsyncDataSystemConfig(synchronizers=[synchronizer]),
    )


@pytest.mark.asyncio
async def test_async_client_starts_in_streaming_mode():
    with start_server() as server:
        with stream_response([always_true_flag]) as stream:
            server.for_path('/sdk/stream', stream)

            async with AsyncLDClient(make_config(server.uri, AsyncStreamingDataSourceBuilder())) as client:
                assert await client.is_initialized()
                assert await client.variation(always_true_flag['key'], user, False) is True

                r = server.await_request()
                assert r.path == '/sdk/stream'
                assert r.headers['Authorization'] == sdk_key


@pytest.mark.asyncio
async def test_async_client_starts_in_polling_mode():
    with start_server() as server:
        server.for_path('/sdk/poll', poll_response([always_true_flag]))

        async with AsyncLDClient(make_config(server.uri, AsyncPollingDataSourceBuilder())) as client:
            assert await client.is_initialized()
            assert await client.variation(always_true_flag['key'], user, False) is True

            r = server.await_request()
            assert r.path == '/sdk/poll'
            assert r.headers['Authorization'] == sdk_key


@pytest.mark.asyncio
async def test_async_client_fails_fast_in_streaming_mode_with_401_error():
    with start_server() as server:
        server.for_path('/sdk/stream', BasicResponse(401))

        start_wait = 10
        client = AsyncLDClient(make_config(server.uri, AsyncStreamingDataSourceBuilder()))
        try:
            started = time.monotonic()
            await client.start(start_wait=start_wait)
            # A 401 turns the data source off, which releases start() early.
            assert time.monotonic() - started < start_wait / 2
            assert not await client.is_initialized()
            assert await client.variation(always_true_flag['key'], user, False) is False
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_async_client_fails_fast_in_polling_mode_with_401_error():
    with start_server() as server:
        server.for_path('/sdk/poll', BasicResponse(401))

        start_wait = 10
        client = AsyncLDClient(make_config(server.uri, AsyncPollingDataSourceBuilder()))
        try:
            started = time.monotonic()
            await client.start(start_wait=start_wait)
            assert time.monotonic() - started < start_wait / 2
            assert not await client.is_initialized()
            assert await client.variation(always_true_flag['key'], user, False) is False
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_async_client_retries_poll_after_network_error(monkeypatch):
    # Lower the minimum poll interval so that the retry happens before the start wait ends.
    monkeypatch.setattr('ldclient.impl.datasource.async_polling.DEFAULT_POLL_INTERVAL', 0.05)
    with start_server() as server:
        server.for_path('/sdk/poll', SequentialHandler(
            CauseNetworkError(), CauseNetworkError(), CauseNetworkError(), poll_response([always_true_flag])))
        builder = AsyncPollingDataSourceBuilder().poll_interval(0.05)

        async with AsyncLDClient(make_config(server.uri, builder)) as client:
            assert await client.is_initialized()
            assert await client.variation(always_true_flag['key'], user, False) is True
