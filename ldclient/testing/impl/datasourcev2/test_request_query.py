"""
The FDv2 data sources send no query parameters except the basis. The FDv1 fallback requesters send
none. In particular, no data source sends a payload filter.
"""

from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest

from ldclient.config import Config
from ldclient.impl.datasource import async_streaming, streaming
from ldclient.impl.datasource.async_polling import (
    AiohttpFDv1PollingRequester,
    AiohttpPollingRequester
)
from ldclient.impl.datasource.polling import (
    Urllib3FDv1PollingRequester,
    Urllib3PollingRequester
)
from ldclient.interfaces import Selector
from ldclient.testing.http_util import start_server
from ldclient.testing.mock_components import MockSelectorStore

SELECTOR = Selector.new_selector('p:SOMETHING:300', 300)


def path_and_query(request):
    parts = urlsplit(request.path)
    return parts.path, parse_qs(parts.query)


def make_config(base_uri: str = 'http://localhost') -> Config:
    return Config('sdk-key', base_uri=base_uri, stream_uri=base_uri)


@pytest.mark.parametrize('selector, expected_query', [
    (None, {}),
    (SELECTOR, {'basis': [SELECTOR.state]}),
])
def test_polling_requester_sends_only_the_basis(selector, expected_query):
    with start_server() as server:
        config = make_config(server.uri)
        requester = Urllib3PollingRequester(config, config.base_uri, config.http)
        requester.fetch(selector)
        assert path_and_query(server.require_request()) == ('/sdk/poll', expected_query)


def test_fdv1_fallback_polling_requester_sends_only_the_basis():
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
async def test_async_polling_requester_sends_only_the_basis(selector, expected_query):
    with start_server() as server:
        config = make_config(server.uri)
        requester = AiohttpPollingRequester(config, config.base_uri, config.http)
        try:
            await requester.fetch(selector)
        finally:
            await requester._http.close()
        assert path_and_query(server.require_request()) == ('/sdk/poll', expected_query)


@pytest.mark.asyncio
async def test_async_fdv1_fallback_polling_requester_sends_only_the_basis():
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
def test_streaming_sends_only_the_basis(selector, expected_params):
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
def test_async_streaming_sends_only_the_basis(selector, expected_params):
    config = make_config()
    with mock.patch.object(async_streaming, 'AsyncSSEFactory') as factory_cls:
        async_streaming.create_sse_client(
            config.stream_base_uri, config.http, config.initial_reconnect_delay, config, MockSelectorStore(selector), session=object()
        )

    args, kwargs = factory_cls.return_value.create.call_args
    assert args[0] == 'http://localhost/sdk/stream'
    assert kwargs['query_params']() == expected_params
