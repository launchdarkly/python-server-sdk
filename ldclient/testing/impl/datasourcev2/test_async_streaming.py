# pylint: disable=missing-docstring, too-few-public-methods

import json
from typing import AsyncIterable, List, Optional
from unittest import mock

import pytest
from ld_eventsource.actions import Start
from ld_eventsource.http import HTTPStatusError
from ld_eventsource.sse_client import Event, Fault

from ldclient.config import Config
from ldclient.impl.datasourcev2 import async_streaming
from ldclient.impl.datasourcev2.async_streaming import (
    STREAMING_ENDPOINT,
    AsyncStreamingDataSource,
    create_sse_client
)
from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    PutObject
)
from ldclient.impl.util import _LD_ENVID_HEADER, _LD_FD_FALLBACK_HEADER
from ldclient.interfaces import (
    ChangeType,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Payload,
    Selector,
    ServerIntent
)
from ldclient.testing.mock_components import MockSelectorStore

# ---------------------------------------------------------------------------
# Mock async SSE client
# ---------------------------------------------------------------------------


class MockAsyncSSEClient:
    """An async SSE client backed by a static list of actions."""

    def __init__(self, actions: List):
        self._actions = actions
        self.interrupted = False
        self.closed = False
        self.next_retry_delay = 0.1

    @property
    def all(self) -> AsyncIterable:
        return self._async_gen()

    async def _async_gen(self):
        for action in self._actions:
            if self.interrupted or self.closed:
                return
            yield action

    async def interrupt(self):
        self.interrupted = True

    async def close(self):
        self.closed = True


def list_sse_client(actions: List):
    """Returns an SseClientBuilder producing a MockAsyncSSEClient."""

    def builder(base_uri, http_options, initial_reconnect_delay, config, ss, session=None):  # pylint: disable=unused-argument
        return MockAsyncSSEClient(actions), None

    return builder


def make_streaming_data_source() -> AsyncStreamingDataSource:
    config = Config("key")
    return AsyncStreamingDataSource(
        config.stream_base_uri + STREAMING_ENDPOINT,
        config.http,
        config.initial_reconnect_delay,
        config,
    )


def server_intent_event(code: IntentCode) -> Event:
    si = ServerIntent(payload=Payload(id="p1", target=1, code=code, reason="test"))
    return Event(event=EventName.SERVER_INTENT, data=json.dumps(si.to_dict()))


def put_object_event(key: str = "my-flag", version: int = 1) -> Event:
    put = PutObject(version=version, kind=ObjectKind.FLAG, key=key, object={"key": key, "version": version})
    return Event(event=EventName.PUT_OBJECT, data=json.dumps(put.to_dict()))


def payload_transferred_event(version: int = 1) -> Event:
    sel = Selector(state=f"p:test:{version}", version=version)
    return Event(event=EventName.PAYLOAD_TRANSFERRED, data=json.dumps(sel.to_dict()))


def delete_object_event(key: str = "my-flag", version: int = 2) -> Event:
    d = DeleteObject(version=version, kind=ObjectKind.FLAG, key=key)
    return Event(event=EventName.DELETE_OBJECT, data=json.dumps(d.to_dict()))


async def collect_updates(src: AsyncStreamingDataSource, actions: List, ss=None):
    """Drive sync() with the given mock actions, collecting yielded updates."""
    if ss is None:
        ss = MockSelectorStore(Selector.no_selector())

    src._sse_client_builder = list_sse_client(actions)

    return [update async for update in src.sync(ss)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_transfer():
    src = make_streaming_data_source()

    actions = [
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    update = updates[0]
    assert update.state == DataSourceState.VALID
    assert update.change_set is not None
    changes = [c for c in update.change_set.changes if c.action == ChangeType.PUT]
    assert any(c.key == "flag-1" for c in changes)


@pytest.mark.asyncio
async def test_transfer_none():
    src = make_streaming_data_source()

    si = ServerIntent(payload=Payload(id="p1", target=1, code=IntentCode.TRANSFER_NONE, reason="up-to-date"))
    actions = [
        Event(event=EventName.SERVER_INTENT, data=json.dumps(si.to_dict())),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_heartbeat_is_ignored():
    src = make_streaming_data_source()

    actions = [
        Event(event=EventName.HEARTBEAT),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_delete_object():
    src = make_streaming_data_source()

    actions = [
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1", 1),
        delete_object_event("flag-1", 2),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    changes = updates[0].change_set.changes
    assert any(c.key == "flag-1" and c.action == ChangeType.PUT for c in changes)
    assert any(c.key == "flag-1" and c.action == ChangeType.DELETE for c in changes)


@pytest.mark.asyncio
async def test_goodbye_is_ignored():
    src = make_streaming_data_source()

    goodbye = Goodbye(reason="test reason")
    actions = [
        Event(event=EventName.GOODBYE, data=json.dumps(goodbye.to_dict())),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_error_resets_changeset():
    src = make_streaming_data_source()

    err = Error(payload_id="p1", reason="test error")
    actions = [
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        # Error mid-transfer — builder is reset
        Event(event=EventName.ERROR, data=json.dumps(err.to_dict())),
        # Re-transmit
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-2"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    changes = [c for c in updates[0].change_set.changes if c.action == ChangeType.PUT]
    keys = {c.key for c in changes}
    assert "flag-2" in keys


@pytest.mark.asyncio
async def test_errorless_fault_is_ignored():
    src = make_streaming_data_source()

    actions = [
        Fault(error=None),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_recoverable_fault_yields_interrupted_and_continues():
    src = make_streaming_data_source()

    actions = [
        Fault(error=HTTPStatusError(503)),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 2
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert updates[0].error.status_code == 503
    assert updates[1].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_unrecoverable_fault_yields_off_and_halts():
    src = make_streaming_data_source()

    actions = [
        Fault(error=HTTPStatusError(401)),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert src._running is False


@pytest.mark.asyncio
async def test_fault_with_fallback_header_halts_with_signal():
    src = make_streaming_data_source()

    headers = {_LD_FD_FALLBACK_HEADER: 'true', _LD_ENVID_HEADER: 'env1'}
    actions = [
        Fault(error=HTTPStatusError(503, headers=headers)),
        server_intent_event(IntentCode.TRANSFER_FULL),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].environment_id == 'env1'


@pytest.mark.asyncio
async def test_fallback_to_fdv1_on_start_header():
    """When Start has X-LD-FD-Fallback: true the next completed update signals fallback."""
    src = make_streaming_data_source()

    fallback_headers = {_LD_FD_FALLBACK_HEADER: 'true', _LD_ENVID_HEADER: 'env1'}

    actions = [
        Start(headers=fallback_headers),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
        # Should never be reached — the latched directive halts the stream.
        put_object_event("flag-2"),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].state == DataSourceState.VALID


@pytest.mark.asyncio
async def test_env_id_propagated():
    """Environment ID from Start headers should be included in updates."""
    src = make_streaming_data_source()

    start_headers = {_LD_ENVID_HEADER: 'my-env'}
    actions = [
        Start(headers=start_headers),
        server_intent_event(IntentCode.TRANSFER_FULL),
        put_object_event("flag-1"),
        payload_transferred_event(),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].environment_id == 'my-env'


@pytest.mark.asyncio
async def test_invalid_json_interrupts_stream():
    src = make_streaming_data_source()

    actions = [
        Event(event=EventName.SERVER_INTENT, data="this is not json"),
    ]

    updates = await collect_updates(src, actions)
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].error.kind == DataSourceErrorKind.INVALID_DATA


@pytest.mark.asyncio
async def test_stop_sets_running_false():
    src = make_streaming_data_source()
    src._running = True
    await src.stop()
    assert src._running is False


@pytest.mark.asyncio
async def test_stop_closes_sse_client():
    src = make_streaming_data_source()
    sse = MockAsyncSSEClient([])
    src._sse = sse
    await src.stop()
    assert sse.closed is True


# ---------------------------------------------------------------------------
# Session ownership
# ---------------------------------------------------------------------------


class FakeSession:
    """Minimal stand-in for an aiohttp.ClientSession that records closure."""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


def test_create_sse_client_builds_session_when_none_supplied():
    config = Config("key")
    ss = MockSelectorStore(Selector.no_selector())
    fake_session = FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ) as make_session, mock.patch.object(
        async_streaming, "AsyncSSEFactory"
    ) as factory_cls:
        factory_cls.return_value.create.return_value = MockAsyncSSEClient([])

        sse, owned = create_sse_client(
            config.stream_base_uri,
            config.http,
            config.initial_reconnect_delay,
            config,
            ss,
        )

    make_session.assert_called_once_with(config, config.http)
    # The configured session is handed to the SSE factory...
    assert factory_cls.call_args.kwargs["session"] is fake_session
    # ...and returned as the owned session the SDK must close.
    assert owned is fake_session


def test_create_sse_client_does_not_build_session_when_supplied():
    config = Config("key")
    ss = MockSelectorStore(Selector.no_selector())
    supplied = FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session"
    ) as make_session, mock.patch.object(
        async_streaming, "AsyncSSEFactory"
    ) as factory_cls:
        factory_cls.return_value.create.return_value = MockAsyncSSEClient([])

        sse, owned = create_sse_client(
            config.stream_base_uri,
            config.http,
            config.initial_reconnect_delay,
            config,
            ss,
            session=supplied,
        )

    make_session.assert_not_called()
    assert factory_cls.call_args.kwargs["session"] is supplied
    # A caller-supplied session is not owned by the SDK.
    assert owned is None


@pytest.mark.asyncio
async def test_owned_session_closed_on_sync_completion():
    """When the SDK creates the session, it is closed once sync() finishes."""
    src = make_streaming_data_source()
    owned = FakeSession()

    def builder(base_uri, http_options, initial_reconnect_delay, config, ss, session=None):  # pylint: disable=unused-argument
        return MockAsyncSSEClient([
            server_intent_event(IntentCode.TRANSFER_FULL),
            put_object_event("flag-1"),
            payload_transferred_event(),
        ]), owned

    src._sse_client_builder = builder
    ss = MockSelectorStore(Selector.no_selector())

    updates = [u async for u in src.sync(ss)]
    assert len(updates) == 1
    assert owned.closed is True
    assert src._owned_session is None


@pytest.mark.asyncio
async def test_owned_session_closed_on_stop():
    """When the SDK creates the session, stop() closes it."""
    src = make_streaming_data_source()
    owned = FakeSession()
    src._sse = MockAsyncSSEClient([])
    src._owned_session = owned

    await src.stop()
    assert owned.closed is True
    assert src._owned_session is None


@pytest.mark.asyncio
async def test_supplied_session_not_closed_on_sync_completion():
    """A caller-supplied session (owned=None) is never closed by the SDK."""
    src = make_streaming_data_source()
    supplied = FakeSession()

    def builder(base_uri, http_options, initial_reconnect_delay, config, ss, session=None):  # pylint: disable=unused-argument
        # Mirrors create_sse_client when a session is supplied: owned is None.
        return MockAsyncSSEClient([
            server_intent_event(IntentCode.TRANSFER_FULL),
            put_object_event("flag-1"),
            payload_transferred_event(),
        ]), None

    src._sse_client_builder = builder
    ss = MockSelectorStore(Selector.no_selector())

    [u async for u in src.sync(ss)]
    assert supplied.closed is False


@pytest.mark.asyncio
async def test_supplied_session_flows_through_builder_as_unowned():
    """A session supplied via the ctor reaches create_sse_client and is not owned."""
    config = Config("key")
    supplied = FakeSession()
    src = AsyncStreamingDataSource(
        config.stream_base_uri + STREAMING_ENDPOINT,
        config.http,
        config.initial_reconnect_delay,
        config,
        session=supplied,
    )

    with mock.patch.object(
        async_streaming, "make_client_session"
    ) as make_session, mock.patch.object(
        async_streaming, "AsyncSSEFactory"
    ) as factory_cls:
        factory_cls.return_value.create.return_value = MockAsyncSSEClient([])
        ss = MockSelectorStore(Selector.no_selector())

        [u async for u in src.sync(ss)]

    make_session.assert_not_called()
    assert factory_cls.call_args.kwargs["session"] is supplied
    assert supplied.closed is False
    assert src._owned_session is None
