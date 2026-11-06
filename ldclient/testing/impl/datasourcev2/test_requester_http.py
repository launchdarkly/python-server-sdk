"""
HTTP behavior of the FDv2 polling requesters and streaming connection: ETag
caching and the request headers they send.
"""

import inspect
from urllib.parse import parse_qs, urlsplit

import pytest

from ldclient.config import Config
from ldclient.impl.datasourcev2 import async_streaming, streaming
from ldclient.impl.datasourcev2.async_polling import (
    AiohttpFDv1PollingRequester,
    AiohttpPollingRequester
)
from ldclient.impl.datasourcev2.polling import (
    Urllib3FDv1PollingRequester,
    Urllib3PollingRequester
)
from ldclient.impl.http import ASYNC_USER_AGENT, SYNC_USER_AGENT
from ldclient.impl.util import _Success
from ldclient.interfaces import IntentCode, Selector
from ldclient.testing.http_util import (
    BasicResponse,
    ChunkedResponse,
    JsonResponse,
    SequentialHandler,
    start_server
)
from ldclient.testing.mock_components import MockSelectorStore
from ldclient.version import VERSION

FDV2_PAYLOAD = {
    'events': [
        {
            'event': 'server-intent',
            'data': {'payloads': [{'id': 'p1', 'target': 461, 'intentCode': 'xfer-full', 'reason': 'payload-missing'}]},
        },
        {
            'event': 'payload-transferred',
            'data': {'state': '(p:p1:461)', 'id': 'p1', 'version': 461},
        },
    ]
}

FDV1_PAYLOAD: dict = {'flags': {}, 'segments': {}}

REQUESTERS = [
    pytest.param(Urllib3PollingRequester, SYNC_USER_AGENT, '/sdk/poll', FDV2_PAYLOAD, id='urllib3-fdv2'),
    pytest.param(Urllib3FDv1PollingRequester, SYNC_USER_AGENT, '/sdk/latest-all', FDV1_PAYLOAD, id='urllib3-fdv1'),
    pytest.param(AiohttpPollingRequester, ASYNC_USER_AGENT, '/sdk/poll', FDV2_PAYLOAD, id='aiohttp-fdv2'),
    pytest.param(AiohttpFDv1PollingRequester, ASYNC_USER_AGENT, '/sdk/latest-all', FDV1_PAYLOAD, id='aiohttp-fdv1'),
]

# Each case gives config options, an instance id, and the optional headers it
# expects. None means the header must be absent.
HEADER_CASES = [
    pytest.param(
        {}, None,
        {'X-LaunchDarkly-Wrapper': None, 'X-LaunchDarkly-Tags': None, 'X-LaunchDarkly-Instance-Id': None},
        id='defaults',
    ),
    pytest.param(
        {'wrapper_name': 'Flask', 'wrapper_version': '0.1.0'}, None,
        {'X-LaunchDarkly-Wrapper': 'Flask/0.1.0'},
        id='wrapper-with-version',
    ),
    pytest.param(
        {'wrapper_name': 'Flask'}, None,
        {'X-LaunchDarkly-Wrapper': 'Flask'},
        id='wrapper-without-version',
    ),
    pytest.param(
        {'application': {'id': 'my-app', 'version': '1.2.3'}}, None,
        {'X-LaunchDarkly-Tags': 'application-id/my-app application-version/1.2.3'},
        id='application-tags',
    ),
    pytest.param(
        {}, 'my-instance-id',
        {'X-LaunchDarkly-Instance-Id': 'my-instance-id'},
        id='instance-id',
    ),
]


def make_config(uri, options=None, instance_id=None):
    config = Config('sdk-key', base_uri=uri, stream_uri=uri, **(options or {}))
    config._instance_id = instance_id
    return config


async def fetch(requester, selector=None):
    # The sync requesters return a result; the async ones return a coroutine.
    result = requester.fetch(selector)
    if inspect.isawaitable(result):
        result = await result
    return result


async def close(requester):
    if hasattr(requester, 'close'):
        await requester.close()
    else:
        requester._http.clear()


def assert_headers(headers, user_agent, expected):
    assert headers.get('Authorization') == 'sdk-key'
    assert headers.get('User-Agent') == user_agent + '/' + VERSION
    for name, value in expected.items():
        assert headers.get(name) == value


@pytest.mark.asyncio
@pytest.mark.parametrize('requester_cls, user_agent, path, payload', REQUESTERS)
async def test_requester_sends_etag_and_returns_no_changes_on_304(requester_cls, user_agent, path, payload):
    with start_server() as server:
        server.for_path(path, SequentialHandler(
            JsonResponse(payload, {'ETag': 'my-etag'}),
            BasicResponse(304),
        ))
        config = make_config(server.uri)
        requester = requester_cls(config, config.base_uri, config.http)
        try:
            first = await fetch(requester)
            second = await fetch(requester)
        finally:
            await close(requester)

        first_req = server.require_request()
        second_req = server.require_request()

    assert first_req.headers.get('If-None-Match') is None
    assert second_req.headers.get('If-None-Match') == 'my-etag'

    assert isinstance(first, _Success)
    assert first.value[0].intent_code == IntentCode.TRANSFER_FULL

    # A 304 means the data is current, so the requester reports no changes.
    assert isinstance(second, _Success)
    change_set = second.value[0]
    assert change_set.intent_code == IntentCode.TRANSFER_NONE
    assert change_set.changes == []
    assert not change_set.selector.is_defined()


@pytest.mark.asyncio
@pytest.mark.parametrize('requester_cls, user_agent, path, payload', REQUESTERS)
async def test_requester_omits_if_none_match_when_response_has_no_etag(requester_cls, user_agent, path, payload):
    with start_server() as server:
        server.for_path(path, JsonResponse(payload))
        config = make_config(server.uri)
        requester = requester_cls(config, config.base_uri, config.http)
        try:
            await fetch(requester)
            await fetch(requester)
        finally:
            await close(requester)

        server.require_request()
        second_req = server.require_request()

    assert second_req.headers.get('If-None-Match') is None


@pytest.mark.asyncio
@pytest.mark.parametrize('options, instance_id, expected', HEADER_CASES)
@pytest.mark.parametrize('requester_cls, user_agent, path, payload', REQUESTERS)
async def test_requester_sends_sdk_headers(requester_cls, user_agent, path, payload, options, instance_id, expected):
    with start_server() as server:
        server.for_path(path, JsonResponse(payload))
        config = make_config(server.uri, options, instance_id)
        requester = requester_cls(config, config.base_uri, config.http)
        try:
            await fetch(requester)
        finally:
            await close(requester)

        req = server.require_request()

    assert req.method == 'GET'
    assert urlsplit(req.path).path == path
    assert parse_qs(urlsplit(req.path).query) == {}
    assert_headers(req.headers, user_agent, expected)


@pytest.mark.parametrize('options, instance_id, expected', HEADER_CASES)
def test_streaming_sends_sdk_headers(options, instance_id, expected):
    with start_server() as server:
        with ChunkedResponse({'Content-Type': 'text/event-stream'}) as stream:
            server.for_path('/sdk/stream', stream)
            config = make_config(server.uri, options, instance_id)
            sse = streaming.create_sse_client(
                config.stream_base_uri, config.http, config.initial_reconnect_delay, config,
                MockSelectorStore(Selector.no_selector()),
            )
            try:
                sse.start()
                req = server.await_request()
            finally:
                sse.close()

    assert req.method == 'GET'
    assert urlsplit(req.path).path == '/sdk/stream'
    assert_headers(req.headers, SYNC_USER_AGENT, expected)


@pytest.mark.asyncio
@pytest.mark.parametrize('options, instance_id, expected', HEADER_CASES)
async def test_async_streaming_sends_sdk_headers(options, instance_id, expected):
    with start_server() as server:
        with ChunkedResponse({'Content-Type': 'text/event-stream'}) as stream:
            server.for_path('/sdk/stream', stream)
            config = make_config(server.uri, options, instance_id)
            sse, session = async_streaming.create_sse_client(
                config.stream_base_uri, config.http, config.initial_reconnect_delay, config,
                MockSelectorStore(Selector.no_selector()),
            )
            try:
                await sse.start()
                req = server.await_request()
            finally:
                await sse.close()
                if session is not None:
                    await session.close()

    assert req.method == 'GET'
    assert urlsplit(req.path).path == '/sdk/stream'
    assert_headers(req.headers, ASYNC_USER_AGENT, expected)
