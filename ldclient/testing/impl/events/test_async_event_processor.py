"""
Tests for AsyncEventProcessor.

These mirror the sync DefaultEventProcessor tests: the processor is driven
through its public API plus the ``_wait_until_inactive`` test handshake, and
HTTP delivery is captured by a mock aiohttp-style session injected through
the ``http`` constructor parameter.
"""

import asyncio
import gzip
import json
import uuid
from contextlib import asynccontextmanager
from email.utils import formatdate
from typing import Any, List, Optional, Tuple

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.context import Context
from ldclient.impl.events.async_event_processor import AsyncEventProcessor
from ldclient.impl.events.diagnostics import (
    _DiagnosticAccumulator,
    create_diagnostic_id
)
from ldclient.impl.events.types import (
    EventInputCustom,
    EventInputEvaluation,
    EventInputIdentify
)
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.stub_util import _MockHTTPHeaderDict

pytestmark = pytest.mark.asyncio

context = Context.builder('userkey').name('Red').build()
flag = FlagBuilder('flagkey').version(2).build()
timestamp = 10000


# ---------------------------------------------------------------------------
# Mock aiohttp session (mirrors stub_util.MockHttp for the aiohttp surface
# used by AsyncHTTPTransport)
# ---------------------------------------------------------------------------

class MockAioResponse:
    def __init__(self, status: int, headers: dict):
        self.status = status
        self.headers = _MockHTTPHeaderDict(headers)

    async def text(self, encoding: str = 'UTF-8', errors: str = 'strict') -> str:
        return ''


class _MockRequestContext:
    def __init__(self, response: MockAioResponse):
        self._response = response

    async def __aenter__(self) -> MockAioResponse:
        return self._response

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        return False


class MockAioHttp:
    def __init__(self):
        self._recorded_requests: List[Tuple[Any, Any]] = []
        self._response_status = 200
        self._server_time: Optional[int] = None

    def request(self, method, uri, headers=None, data=None, timeout=None, proxy=None):
        self._recorded_requests.append((headers, data))
        resp_hdr = dict()
        if self._server_time is not None:
            resp_hdr['date'] = formatdate(self._server_time / 1000, localtime=False, usegmt=True)
        return _MockRequestContext(MockAioResponse(self._response_status, resp_hdr))

    @property
    def request_data(self):
        if len(self._recorded_requests) != 0:
            return self._recorded_requests[-1][1]

    @property
    def request_headers(self):
        if len(self._recorded_requests) != 0:
            return self._recorded_requests[-1][0]

    @property
    def recorded_requests(self):
        return self._recorded_requests

    def set_response_status(self, status: int):
        self._response_status = status

    def set_server_time(self, timestamp: int):
        self._server_time = timestamp

    def reset(self):
        self._recorded_requests = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@asynccontextmanager
async def make_processor(mock_http: MockAioHttp, **kwargs):
    kwargs.setdefault('diagnostic_opt_out', True)
    kwargs.setdefault('sdk_key', 'SDK_KEY')
    config = AsyncConfig(**kwargs)
    diagnostic_accumulator = _DiagnosticAccumulator(create_diagnostic_id(config))
    ep = AsyncEventProcessor(config, mock_http, diagnostic_accumulator=diagnostic_accumulator)
    try:
        yield ep
    finally:
        await ep.stop()


async def flush_and_get_events(ep: AsyncEventProcessor, mock_http: MockAioHttp):
    ep.flush()
    await ep._wait_until_inactive()
    if mock_http.request_data is None:
        raise AssertionError('Expected to get an HTTP request but did not get one')
    return json.loads(mock_http.request_data)


# ---------------------------------------------------------------------------
# Event payload tests
# ---------------------------------------------------------------------------

async def test_identify_event_is_queued():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        e = EventInputIdentify(timestamp, context)
        ep.send_event(e)

        output = await flush_and_get_events(ep, mock_http)
        assert len(output) == 1
        assert output[0] == {'kind': 'identify', 'creationDate': timestamp, 'context': context.to_dict()}


async def test_individual_feature_event_is_queued_with_index_event():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        e = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = await flush_and_get_events(ep, mock_http)
        assert len(output) == 3
        assert output[0]['kind'] == 'index'
        assert output[0]['context'] == context.to_dict()
        assert output[1]['kind'] == 'feature'
        assert output[1]['key'] == flag.key
        assert output[1]['value'] == 'value'
        assert output[2]['kind'] == 'summary'


async def test_custom_event_is_queued():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        e = EventInputCustom(timestamp, context, 'eventkey', {'thing': 'stuff'}, 1.5)
        ep.send_event(e)

        output = await flush_and_get_events(ep, mock_http)
        assert len(output) == 2
        assert output[0]['kind'] == 'index'
        assert output[1]['kind'] == 'custom'
        assert output[1]['key'] == 'eventkey'
        assert output[1]['data'] == {'thing': 'stuff'}
        assert output[1]['metricValue'] == 1.5


async def test_two_events_for_same_context_only_produce_one_index_event():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        e0 = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value1', None, 'default', None, True)
        e1 = EventInputEvaluation(timestamp, context, flag.key, flag, 2, 'value2', None, 'default', None, True)
        ep.send_event(e0)
        ep.send_event(e1)

        output = await flush_and_get_events(ep, mock_http)
        assert len(output) == 4
        assert output[0]['kind'] == 'index'
        assert output[1]['kind'] == 'feature'
        assert output[2]['kind'] == 'feature'
        assert output[3]['kind'] == 'summary'


async def test_nontracked_events_are_summarized():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        e = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value', None, 'default', None, False)
        ep.send_event(e)

        output = await flush_and_get_events(ep, mock_http)
        assert len(output) == 2
        assert output[0]['kind'] == 'index'
        assert output[1]['kind'] == 'summary'
        assert flag.key in output[1]['features']


async def test_nothing_is_sent_if_there_are_no_events():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        ep.flush()
        await ep._wait_until_inactive()
        assert mock_http.request_data is None


async def test_stop_flushes_remaining_events():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
    # stop() (via the context manager) triggers a final flush
    assert mock_http.request_data is not None
    output = json.loads(mock_http.request_data)
    assert output[0]['kind'] == 'identify'


# ---------------------------------------------------------------------------
# Header tests
# ---------------------------------------------------------------------------

async def test_sdk_key_is_sent():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, sdk_key='SDK_KEY') as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        await flush_and_get_events(ep, mock_http)

        assert mock_http.request_headers.get('Authorization') == 'SDK_KEY'


async def test_event_schema_set_on_event_send():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        await flush_and_get_events(ep, mock_http)

        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') == "4"


async def test_event_payload_id_is_sent():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        await flush_and_get_events(ep, mock_http)

        header_val = mock_http.request_headers.get('X-LaunchDarkly-Payload-ID')
        assert header_val is not None
        # Throws on invalid UUID
        uuid.UUID(header_val)


async def test_event_payload_id_changes_between_requests():
    mock_http = MockAioHttp()
    async with make_processor(mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        await flush_and_get_events(ep, mock_http)

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()

        first_payload_id = mock_http.recorded_requests[0][0].get('X-LaunchDarkly-Payload-ID')
        second_payload_id = mock_http.recorded_requests[1][0].get('X-LaunchDarkly-Payload-ID')
        assert first_payload_id != second_payload_id


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

async def test_init_diagnostic_event_sent():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, diagnostic_opt_out=False) as ep:
        diag_init = await flush_and_get_events(ep, mock_http)
        assert len(diag_init) == 6
        assert diag_init['kind'] == 'diagnostic-init'


async def test_periodic_diagnostic_includes_events_in_batch():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, diagnostic_opt_out=False) as ep:
        # Ignore init event
        await flush_and_get_events(ep, mock_http)
        # Send a payload with a single event
        ep.send_event(EventInputIdentify(timestamp, context))
        await flush_and_get_events(ep, mock_http)

        await ep._send_diagnostic()
        diag_event = await flush_and_get_events(ep, mock_http)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 1
        assert diag_event['deduplicatedUsers'] == 0


async def test_periodic_diagnostic_includes_deduplicated_users():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, diagnostic_opt_out=False) as ep:
        # Ignore init event
        await flush_and_get_events(ep, mock_http)
        # Send two custom events with the same user to cause a user deduplication
        e0 = EventInputCustom(timestamp, context, 'event1', None, None)
        e1 = EventInputCustom(timestamp, context, 'event2', None, None)
        ep.send_event(e0)
        ep.send_event(e1)
        await flush_and_get_events(ep, mock_http)

        await ep._send_diagnostic()
        diag_event = await flush_and_get_events(ep, mock_http)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 3
        assert diag_event['deduplicatedUsers'] == 1


async def test_event_schema_not_set_on_diagnostic_send():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, diagnostic_opt_out=False) as ep:
        await ep._wait_until_inactive()
        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') is None


# ---------------------------------------------------------------------------
# HTTP error handling
# ---------------------------------------------------------------------------

async def verify_unrecoverable_http_error(status: int):
    mock_http = MockAioHttp()
    async with make_processor(mock_http, sdk_key='SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()
        assert mock_http.request_data is None


async def verify_recoverable_http_error(status: int):
    mock_http = MockAioHttp()
    async with make_processor(mock_http, sdk_key='SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()
        assert mock_http.request_data is not None


async def test_no_more_payloads_are_sent_after_401_error():
    await verify_unrecoverable_http_error(401)


async def test_no_more_payloads_are_sent_after_403_error():
    await verify_unrecoverable_http_error(403)


async def test_will_still_send_after_408_error():
    await verify_recoverable_http_error(408)


async def test_will_still_send_after_429_error():
    await verify_recoverable_http_error(429)


async def test_will_still_send_after_500_error():
    await verify_recoverable_http_error(500)


# ---------------------------------------------------------------------------
# Inbox capacity
# ---------------------------------------------------------------------------

async def test_does_not_block_on_full_inbox():
    config = AsyncConfig("fake_sdk_key", events_max_pending=1)  # this sets the size of both the inbox and the outbox to 1
    ep_inbox_holder = [None]

    def dispatcher_factory(inbox, config, http, diag):
        ep_inbox_holder[0] = inbox  # it's an array because otherwise it's hard for a closure to modify a variable
        return None  # the dispatcher object itself doesn't matter, we only manipulate the inbox

    async def event_consumer():
        while True:
            message = await ep_inbox.get()
            if message.type == 'stop':
                message.param.set()
                return

    mock_http = MockAioHttp()
    ep = AsyncEventProcessor(config, mock_http, dispatcher_factory)
    ep_inbox = ep_inbox_holder[0]
    event1 = EventInputCustom(timestamp, context, 'event1')
    event2 = EventInputCustom(timestamp, context, 'event2')
    ep.send_event(event1)
    ep.send_event(event2)  # this event should be dropped - inbox is full
    message1 = await ep_inbox.get(block=False)
    had_no_more = ep_inbox.empty()
    consumer = asyncio.ensure_future(event_consumer())
    await ep.stop()
    await consumer
    assert message1.param == event1
    assert had_no_more


# ---------------------------------------------------------------------------
# Compression
# ---------------------------------------------------------------------------

async def test_event_payload_is_gzip_compressed_when_enabled():
    mock_http = MockAioHttp()
    async with make_processor(mock_http, enable_event_compression=True) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        await ep._wait_until_inactive()

        assert mock_http.request_headers.get('Content-Encoding') == 'gzip'
        output = json.loads(gzip.decompress(mock_http.request_data))
        assert len(output) == 1
        assert output[0]['kind'] == 'identify'
