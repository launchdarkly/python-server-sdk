"""
The FDv2 data system does not support payload filtering. These tests show that its data sources do
not send the payload filter key, also when the config has one. This includes the FDv1 fallback
requesters, so that a fallback does not change the client to a filtered subset of the data.
"""

from unittest import mock
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
from ldclient.interfaces import Selector
from ldclient.testing.http_util import start_server
from ldclient.testing.mock_components import MockSelectorStore

FILTER_KEY = 'microservice-1'
SELECTOR = Selector.new_selector('p:SOMETHING:300', 300)


def path_and_query(request):
    parts = urlsplit(request.path)
    return parts.path, parse_qs(parts.query)


def make_config(base_uri: str = 'http://localhost') -> Config:
    return Config('sdk-key', base_uri=base_uri, stream_uri=base_uri, payload_filter_key=FILTER_KEY)


@pytest.mark.parametrize('selector, expected_query', [
    (None, {}),
    (SELECTOR, {'basis': [SELECTOR.state]}),
])
def test_polling_requester_does_not_send_the_filter(selector, expected_query):
    with start_server() as server:
        config = make_config(server.uri)
        requester = Urllib3PollingRequester(config, config.base_uri, config.http)
        requester.fetch(selector)
        assert path_and_query(server.require_request()) == ('/sdk/poll', expected_query)


def test_fdv1_fallback_polling_requester_does_not_send_the_filter():
    with start_server() as server:
        config = make_config(server.uri)
        requester = Urllib3FDv1PollingRequester(config, config.base_uri, config.http)
        requester.fetch(SELECTOR)
        assert path_and_query(server.require_request()) == ('/sdk/latest-all', {})


@pytest.mark.asyncio
@pytest.mark.parametrize('selector, expected_query', [
    (None, {}),
    (SELECTOR, {'basis': [SELECTOR.state]}),
])
async def test_async_polling_requester_does_not_send_the_filter(selector, expected_query):
    with start_server() as server:
        config = make_config(server.uri)
        requester = AiohttpPollingRequester(config, config.base_uri, config.http)
        try:
            await requester.fetch(selector)
        finally:
            await requester._http.close()
        assert path_and_query(server.require_request()) == ('/sdk/poll', expected_query)


@pytest.mark.asyncio
async def test_async_fdv1_fallback_polling_requester_does_not_send_the_filter():
    with start_server() as server:
        config = make_config(server.uri)
        requester = AiohttpFDv1PollingRequester(config, config.base_uri, config.http)
        try:
            await requester.fetch(SELECTOR)
        finally:
            await requester._http.close()
        assert path_and_query(server.require_request()) == ('/sdk/latest-all', {})


@pytest.mark.parametrize('selector, expected_params', [
    (Selector.no_selector(), {}),
    (SELECTOR, {'basis': SELECTOR.state}),
])
def test_streaming_does_not_send_the_filter(selector, expected_params):
    config = make_config()
    with mock.patch.object(streaming.ConnectStrategy, 'http', wraps=streaming.ConnectStrategy.http) as http:
        streaming.create_sse_client(config.stream_base_uri, config.http, config.initial_reconnect_delay, config, MockSelectorStore(selector))

    kwargs = http.call_args.kwargs
    assert kwargs['url'] == 'http://localhost/sdk/stream'
    assert kwargs['query_params']() == expected_params


@pytest.mark.parametrize('selector, expected_params', [
    (Selector.no_selector(), {}),
    (SELECTOR, {'basis': SELECTOR.state}),
])
def test_async_streaming_does_not_send_the_filter(selector, expected_params):
    config = make_config()
    with mock.patch.object(async_streaming, 'AsyncSSEFactory') as factory_cls:
        async_streaming.create_sse_client(
            config.stream_base_uri, config.http, config.initial_reconnect_delay, config, MockSelectorStore(selector), session=object()
        )

    args, kwargs = factory_cls.return_value.create.call_args
    assert args[0] == 'http://localhost/sdk/stream'
    assert kwargs['query_params']() == expected_params
