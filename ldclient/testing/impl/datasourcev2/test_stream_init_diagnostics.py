# pylint: disable=missing-docstring, too-few-public-methods
"""
The FDv2 streaming synchronizers record stream inits through their diagnostic
accumulator. These tests drive sync() with mock SSE clients and read the
recorded inits back from a real _DiagnosticAccumulator.
"""

import json
from typing import Iterable, List

import pytest
from ld_eventsource.actions import Action, Start
from ld_eventsource.http import HTTPStatusError
from ld_eventsource.sse_client import Event, Fault

from ldclient.config import Config
from ldclient.impl.datasourcev2 import async_streaming, streaming
from ldclient.impl.datasourcev2.async_streaming import AsyncStreamingDataSource
from ldclient.impl.datasourcev2.streaming import (
    STREAMING_ENDPOINT,
    StreamingDataSource
)
from ldclient.impl.datasystem.protocolv2 import EventName
from ldclient.impl.events.diagnostics import _DiagnosticAccumulator
from ldclient.interfaces import (
    DataSourceState,
    IntentCode,
    Payload,
    Selector,
    ServerIntent
)
from ldclient.testing.mock_components import MockSelectorStore


class ListBasedSseClient:
    """A sync SSE client that replays a fixed sequence of actions."""

    def __init__(self, actions: Iterable[Action]):
        self._actions = actions

    @property
    def all(self) -> Iterable[Action]:
        return self._actions

    @property
    def next_retry_delay(self):
        return 1

    def interrupt(self):
        pass

    def close(self):
        pass


class MockAsyncSSEClient:
    """An async SSE client that replays a fixed sequence of actions."""

    def __init__(self, actions: Iterable[Action]):
        self._actions = actions
        self.closed = False
        self.next_retry_delay = 1

    @property
    def all(self):
        return self._gen()

    async def _gen(self):
        for action in self._actions:
            if self.closed:
                return
            yield action

    async def interrupt(self):
        pass

    async def close(self):
        self.closed = True


def _sync_source(actions: Iterable[Action], accumulator) -> StreamingDataSource:
    config = Config("key")
    source = StreamingDataSource(
        config.stream_base_uri + STREAMING_ENDPOINT,
        config.http,
        config.initial_reconnect_delay,
        config,
    )
    source._sse_client_builder = lambda *args, **kwargs: ListBasedSseClient(actions)
    source.set_diagnostic_accumulator(accumulator)
    return source


def _async_source(actions: Iterable[Action], accumulator) -> AsyncStreamingDataSource:
    config = Config("key")
    source = AsyncStreamingDataSource(
        config.stream_base_uri + STREAMING_ENDPOINT,
        config.http,
        config.initial_reconnect_delay,
        config,
    )
    source._sse_client_builder = lambda *args, **kwargs: (MockAsyncSSEClient(actions), None)
    source.set_diagnostic_accumulator(accumulator)
    return source


def _run_sync(actions: Iterable[Action], accumulator) -> list:
    source = _sync_source(actions, accumulator)
    return list(source.sync(MockSelectorStore(Selector.no_selector())))


async def _run_async(actions: Iterable[Action], accumulator) -> list:
    source = _async_source(actions, accumulator)
    return [u async for u in source.sync(MockSelectorStore(Selector.no_selector()))]


def _inits(accumulator) -> List[dict]:
    return accumulator.create_event_and_reset(0, 0)['streamInits']


def _intent(code: IntentCode = IntentCode.TRANSFER_FULL) -> Event:
    si = ServerIntent(payload=Payload(id="p1", target=1, code=code, reason="test"))
    return Event(event=EventName.SERVER_INTENT, data=json.dumps(si.to_dict()))


def _transferred(version: int = 1) -> Event:
    sel = Selector(state=f"p:test:{version}", version=version)
    return Event(event=EventName.PAYLOAD_TRANSFERRED, data=json.dumps(sel.to_dict()))


def _full_payload() -> List[Action]:
    return [Start(), _intent(), _transferred()]


class _FakeClock:
    """Holds a monotonic clock and a wall clock the test steps by hand."""

    def __init__(self):
        self.monotonic = 100.0
        self.wall_millis = 5_000_000

    def install(self, monkeypatch, module):
        monkeypatch.setattr(module, "monotonic_seconds", lambda: self.monotonic)
        monkeypatch.setattr(module, "current_time_millis", lambda: self.wall_millis)


def _clock_stepping_actions(clock: _FakeClock):
    # The generator steps both clocks between the intent and the payload end.
    # The monotonic clock advances two seconds. The wall clock jumps back an hour.
    yield Start()
    yield _intent()
    clock.monotonic += 2.0
    clock.wall_millis -= 3_600_000
    yield _transferred()


# Sync synchronizer


def test_sync_records_successful_init_when_first_payload_arrives():
    accumulator = _DiagnosticAccumulator(1)

    updates = _run_sync(_full_payload(), accumulator)

    assert updates[0].state == DataSourceState.VALID
    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['failed'] is False
    assert inits[0]['durationMillis'] >= 0


def test_sync_records_successful_init_for_transfer_none_intent():
    accumulator = _DiagnosticAccumulator(1)

    _run_sync([Start(), _intent(IntentCode.TRANSFER_NONE)], accumulator)

    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['failed'] is False


def test_sync_start_action_alone_records_nothing():
    accumulator = _DiagnosticAccumulator(1)

    _run_sync([Start(), _intent()], accumulator)

    assert _inits(accumulator) == []


def test_sync_records_one_init_across_multiple_payloads():
    accumulator = _DiagnosticAccumulator(1)

    _run_sync(_full_payload() + [_intent(), _transferred(2)], accumulator)

    assert len(_inits(accumulator)) == 1


def test_sync_records_failed_init_on_recoverable_http_error_then_success():
    accumulator = _DiagnosticAccumulator(1)

    _run_sync([Fault(error=HTTPStatusError(503))] + _full_payload(), accumulator)

    inits = _inits(accumulator)
    assert [i['failed'] for i in inits] == [True, False]
    assert all(i['durationMillis'] >= 0 for i in inits)


def test_sync_records_failed_init_on_unrecoverable_http_error():
    accumulator = _DiagnosticAccumulator(1)

    updates = _run_sync([Fault(error=HTTPStatusError(401))] + _full_payload(), accumulator)

    assert updates[-1].state == DataSourceState.OFF
    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['failed'] is True


def test_sync_records_failed_init_on_network_fault():
    accumulator = _DiagnosticAccumulator(1)

    _run_sync([Fault(error=ConnectionError("connection refused"))] + _full_payload(), accumulator)

    inits = _inits(accumulator)
    assert [i['failed'] for i in inits] == [True, False]


def test_sync_init_duration_uses_monotonic_clock_across_wall_clock_step(monkeypatch):
    clock = _FakeClock()
    clock.install(monkeypatch, streaming)
    accumulator = _DiagnosticAccumulator(1)

    _run_sync(_clock_stepping_actions(clock), accumulator)

    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['durationMillis'] == 2000
    assert inits[0]['timestamp'] == clock.wall_millis
    assert inits[0]['failed'] is False


def test_sync_reconnect_after_clean_close_records_no_second_init():
    # A Fault with no error is ignored, so the baseline stays cleared.
    # The reconnect's payload records nothing.
    accumulator = _DiagnosticAccumulator(1)

    _run_sync(_full_payload() + [Fault(error=None)] + _full_payload(), accumulator)

    assert len(_inits(accumulator)) == 1


# Async synchronizer


@pytest.mark.asyncio
async def test_async_records_successful_init_when_first_payload_arrives():
    accumulator = _DiagnosticAccumulator(1)

    updates = await _run_async(_full_payload(), accumulator)

    assert updates[0].state == DataSourceState.VALID
    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['failed'] is False
    assert inits[0]['durationMillis'] >= 0


@pytest.mark.asyncio
async def test_async_start_action_alone_records_nothing():
    accumulator = _DiagnosticAccumulator(1)

    await _run_async([Start(), _intent()], accumulator)

    assert _inits(accumulator) == []


@pytest.mark.asyncio
async def test_async_records_one_init_across_multiple_payloads():
    accumulator = _DiagnosticAccumulator(1)

    await _run_async(_full_payload() + [_intent(), _transferred(2)], accumulator)

    assert len(_inits(accumulator)) == 1


@pytest.mark.asyncio
async def test_async_records_failed_init_on_recoverable_http_error_then_success():
    accumulator = _DiagnosticAccumulator(1)

    await _run_async([Fault(error=HTTPStatusError(503))] + _full_payload(), accumulator)

    inits = _inits(accumulator)
    assert [i['failed'] for i in inits] == [True, False]
    assert all(i['durationMillis'] >= 0 for i in inits)


@pytest.mark.asyncio
async def test_async_records_failed_init_on_unrecoverable_http_error():
    accumulator = _DiagnosticAccumulator(1)

    updates = await _run_async([Fault(error=HTTPStatusError(401))] + _full_payload(), accumulator)

    assert updates[-1].state == DataSourceState.OFF
    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['failed'] is True


@pytest.mark.asyncio
async def test_async_records_failed_init_on_network_fault():
    accumulator = _DiagnosticAccumulator(1)

    await _run_async([Fault(error=ConnectionError("connection refused"))] + _full_payload(), accumulator)

    inits = _inits(accumulator)
    assert [i['failed'] for i in inits] == [True, False]


@pytest.mark.asyncio
async def test_async_init_duration_uses_monotonic_clock_across_wall_clock_step(monkeypatch):
    clock = _FakeClock()
    clock.install(monkeypatch, async_streaming)
    accumulator = _DiagnosticAccumulator(1)

    await _run_async(_clock_stepping_actions(clock), accumulator)

    inits = _inits(accumulator)
    assert len(inits) == 1
    assert inits[0]['durationMillis'] == 2000
    assert inits[0]['timestamp'] == clock.wall_millis
    assert inits[0]['failed'] is False


@pytest.mark.asyncio
async def test_async_reconnect_after_clean_close_records_no_second_init():
    # A Fault with no error is ignored, so the baseline stays cleared.
    # The reconnect's payload records nothing.
    accumulator = _DiagnosticAccumulator(1)

    await _run_async(_full_payload() + [Fault(error=None)] + _full_payload(), accumulator)

    assert len(_inits(accumulator)) == 1
