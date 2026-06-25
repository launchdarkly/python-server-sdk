"""
Tests for AsyncStreamingUpdateProcessor.

These tests inject a mock SSE factory that yields pre-configured actions
rather than making real network connections.
"""

import asyncio
import json
from unittest import mock

import pytest

from ldclient.config import Config
from ldclient.impl.datasource import async_streaming
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingUpdateProcessor
)
from ldclient.impl.model import ModelEntity
from ldclient.interfaces import DataSourceErrorKind, DataSourceState
from ldclient.testing.builders import FlagBuilder, SegmentBuilder
from ldclient.testing.mock_async_components import MockAsyncFeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


def _item_dict(item):
    """Convert a model entity (FeatureFlag, Segment, etc.) to a plain dict."""
    return item.to_json_dict() if isinstance(item, ModelEntity) else item


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kwargs):
    return Config(sdk_key='sdk-key', **kwargs)


def _make_put_data(flags=None, segments=None):
    flags = flags or {}
    segments = segments or {}
    return json.dumps({"data": {"flags": flags, "segments": segments}})


def _make_patch_data(kind, item):
    path = '%s%s' % (kind.stream_api_path, item['key'])
    return json.dumps({"path": path, "data": item})


def _make_delete_data(kind, key, version):
    path = '%s%s' % (kind.stream_api_path, key)
    return json.dumps({"path": path, "version": version})


# The processor matches actions with isinstance() against the real
# ld_eventsource action classes, so the fakes must be real instances.
from ld_eventsource.actions import Event as _RealEvent  # noqa: E402
from ld_eventsource.actions import Fault as _RealFault  # noqa: E402
from ld_eventsource.actions import Start as _RealStart  # noqa: E402


def _event(event_type: str, data: str) -> _RealEvent:
    return _RealEvent(event=event_type, data=data)


def _fault(error=None) -> _RealFault:
    return _RealFault(error=error)


def _start() -> _RealStart:
    return _RealStart(headers={})


async def _actions_generator(actions: list):
    """Yield a fixed sequence of actions then hang (simulates a live stream)."""
    for action in actions:
        yield action
    # Block forever so the processor's loop doesn't exit until cancelled.
    await asyncio.Event().wait()


class _MockSSE:
    """Stand-in for AsyncSSEClient exposing the surface the processor uses."""

    def __init__(self, actions: list):
        self._actions = actions
        self.interrupted = False
        self.closed = False
        self.next_retry_delay = 0.0

    async def interrupt(self):
        self.interrupted = True

    async def close(self):
        self.closed = True

    @property
    def all(self):
        return _actions_generator(self._actions)


class _MockSSEFactory:
    """Stand-in for AsyncSSEFactory; create() returns a _MockSSE."""

    def __init__(self, actions: list):
        self._actions = actions
        self.created: list = []

    def create(self, url: str, initial_retry_delay: float) -> _MockSSE:
        sse = _MockSSE(self._actions)
        self.created.append(sse)
        return sse


def _make_processor(actions, config=None, store=None, ready_event=None, diag=None):
    config = config or _make_config()
    store = store or MockAsyncFeatureStore()
    ready_event = ready_event or asyncio.Event()
    factory = _MockSSEFactory(actions)
    proc = AsyncStreamingUpdateProcessor(config, store, ready_event, diag, factory)
    return proc, store, ready_event, factory


async def _run_with_actions(actions: list, config=None, store=None, ready_event=None,
                            diag=None, extra_ready_timeout=3.0):
    """Run the processor against a fake SSE action sequence.

    Starts the processor and waits for the ready event (up to
    *extra_ready_timeout* seconds), then returns
    ``(processor, store, ready_event, factory)``.
    """
    proc, store, ready, factory = _make_processor(actions, config, store, ready_event, diag)
    proc.start()
    try:
        await asyncio.wait_for(ready.wait(), timeout=extra_ready_timeout)
    except asyncio.TimeoutError:
        pass  # some tests expect ready NOT to be set
    return proc, store, ready, factory


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_event_initializes_store_and_sets_ready():
    flag = FlagBuilder('f1').version(1).build()
    segment = SegmentBuilder('s1').version(1).build()
    put_data = _make_put_data(
        flags={'f1': _item_dict(flag)},
        segments={'s1': _item_dict(segment)},
    )
    actions = [_start(), _event('put', put_data)]

    proc, store, ready, _ = await _run_with_actions(actions)

    assert ready.is_set()
    assert store.initialized
    stored_flag = await store.get(FEATURES, 'f1')
    assert stored_flag is not None
    assert stored_flag['version'] == 1

    stored_seg = await store.get(SEGMENTS, 's1')
    assert stored_seg is not None
    assert stored_seg['version'] == 1

    await proc.stop()


@pytest.mark.asyncio
async def test_patch_event_upserts_to_store():
    flagv1 = FlagBuilder('f1').version(1).build()
    flagv2 = FlagBuilder('f1').version(2).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flagv1)})
    patch_data = _make_patch_data(FEATURES, _item_dict(flagv2))
    actions = [_start(), _event('put', put_data), _event('patch', patch_data)]

    proc, store, ready, _ = await _run_with_actions(actions)

    # Give the event loop a beat for the patch to process after ready fires.
    await asyncio.sleep(0.05)

    stored = await store.get(FEATURES, 'f1')
    assert stored is not None
    assert stored['version'] == 2

    await proc.stop()


@pytest.mark.asyncio
async def test_delete_event_removes_item_from_store():
    flagv1 = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flagv1)})
    delete_data = _make_delete_data(FEATURES, 'f1', 2)
    actions = [_start(), _event('put', put_data), _event('delete', delete_data)]

    proc, store, ready, _ = await _run_with_actions(actions)

    await asyncio.sleep(0.05)

    deleted = await store.get(FEATURES, 'f1')
    assert deleted is None

    await proc.stop()


@pytest.mark.asyncio
async def test_fault_with_error_does_not_set_ready_by_itself():
    """A Fault that arrives before any put must not mark the processor as initialized."""
    from ld_eventsource.errors import HTTPStatusError

    # Provide only a recoverable fault (503) — no put follows.
    actions = [_start(), _fault(error=HTTPStatusError(503))]

    # Use a short timeout so the test doesn't hang.
    proc, store, ready, _ = await _run_with_actions(actions, extra_ready_timeout=0.2)

    # Ready should NOT have been set by the fault alone.
    assert not ready.is_set()
    assert not store.initialized

    await proc.stop()


@pytest.mark.asyncio
async def test_fault_none_error_is_ignored():
    """A Fault with error=None (clean close) should not update status or stop the processor."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [
        _start(),
        _event('put', put_data),
        _fault(error=None),  # clean close — should be ignored
    ]

    proc, store, ready, _ = await _run_with_actions(actions)

    assert ready.is_set()
    assert store.initialized

    await proc.stop()


@pytest.mark.asyncio
async def test_unrecoverable_http_error_stops_processor():
    """An unrecoverable HTTP status closes the stream and reports OFF with error info."""
    from ld_eventsource.errors import HTTPStatusError

    from ldclient.impl.datasource.async_status import (
        AsyncDataSourceUpdateSinkImpl
    )
    from ldclient.impl.listeners import Listeners

    store = MockAsyncFeatureStore()
    statuses = []
    listeners = Listeners()
    listeners.add(lambda s: statuses.append(s))

    config = _make_config()
    config._data_source_update_sink = AsyncDataSourceUpdateSinkImpl(store, listeners, Listeners())

    actions = [_start(), _fault(error=HTTPStatusError(401))]

    proc, store, ready, factory = await _run_with_actions(actions, config=config, store=store)

    # The unrecoverable error unblocks initialization without initializing the store.
    assert ready.is_set()
    assert not proc.initialized()
    assert factory.created[0].closed
    assert any(
        s.state == DataSourceState.OFF
        and s.error is not None
        and s.error.kind == DataSourceErrorKind.ERROR_RESPONSE
        and s.error.status_code == 401
        for s in statuses
    )

    await proc.stop()


@pytest.mark.asyncio
async def test_stop_closes_sse_and_finishes_task():
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [_start(), _event('put', put_data)]

    proc, store, ready, factory = await _run_with_actions(actions)
    assert ready.is_set()

    await proc.stop()

    # After stop() the SSE client is closed and no background task remains.
    assert factory.created[0].closed
    assert len(proc._runner._tasks) == 0


@pytest.mark.asyncio
async def test_second_start_raises():
    actions = [_start()]
    proc, store, ready, _ = _make_processor(actions)
    proc.start()
    try:
        with pytest.raises(RuntimeError):
            proc.start()
    finally:
        await proc.stop()


@pytest.mark.asyncio
async def test_invalid_json_triggers_invalid_data_status():
    from ldclient.impl.datasource.async_status import (
        AsyncDataSourceUpdateSinkImpl
    )
    from ldclient.impl.listeners import Listeners

    store = MockAsyncFeatureStore()
    listeners = Listeners()
    statuses = []
    listeners.add(lambda s: statuses.append(s))

    config = _make_config()
    config._data_source_update_sink = AsyncDataSourceUpdateSinkImpl(store, listeners, Listeners())

    # Deliver a put first so the processor initializes, then a bad patch.
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    bad_patch_data = 'not valid json'
    actions = [
        _start(),
        _event('put', put_data),
        _event('patch', bad_patch_data),
    ]

    proc, store, ready, _ = await _run_with_actions(actions, config=config, store=store)
    await asyncio.sleep(0.1)  # let patch event propagate

    error_statuses = [s for s in statuses if s.error is not None]
    assert any(
        s.error.kind == DataSourceErrorKind.INVALID_DATA for s in error_statuses
    ), "Expected INVALID_DATA status from bad JSON"

    await proc.stop()


@pytest.mark.asyncio
async def test_patch_unknown_path_is_ignored():
    """A patch for an unknown path should log a warning and not crash."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    bad_patch = json.dumps({"path": "/unknown/something", "data": {"key": "x", "version": 1}})
    actions = [_start(), _event('put', put_data), _event('patch', bad_patch)]

    proc, store, ready, _ = await _run_with_actions(actions)
    await asyncio.sleep(0.05)

    # Store should be unchanged (still has f1 at version 1).
    stored = await store.get(FEATURES, 'f1')
    assert stored is not None
    assert stored['version'] == 1

    await proc.stop()


@pytest.mark.asyncio
async def test_initialized_reflects_store_state():
    proc, store, ready, _ = _make_processor([])
    assert not proc.initialized()

    proc.start()
    # Seed the store and set ready manually to simulate post-put state.
    flag = FlagBuilder('f1').version(1).build()
    await store.init({FEATURES: {'f1': _item_dict(flag)}, SEGMENTS: {}})
    ready.set()
    proc._running = True

    assert proc.initialized()

    await proc.stop()


# ---------------------------------------------------------------------------
# Session ownership
# ---------------------------------------------------------------------------


class _FakeSession:
    """Minimal stand-in for an aiohttp.ClientSession that records closure."""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_default_construction_builds_configured_session():
    """With no injected factory, a configured session is built via
    make_client_session and handed to the SSE factory."""
    config = _make_config()
    store = MockAsyncFeatureStore()
    ready = asyncio.Event()
    fake_session = _FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ) as make_session, mock.patch.object(
        async_streaming, "AsyncSSEFactory"
    ) as factory_cls:
        proc = AsyncStreamingUpdateProcessor(config, store, ready, None)

    make_session.assert_called_once_with(config)
    assert factory_cls.call_args.kwargs["session"] is fake_session
    assert proc._owned_session is fake_session


@pytest.mark.asyncio
async def test_default_construction_session_closed_on_stop():
    """The SDK-created session is closed when the processor stops."""
    config = _make_config()
    store = MockAsyncFeatureStore()
    ready = asyncio.Event()
    fake_session = _FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ), mock.patch.object(async_streaming, "AsyncSSEFactory"):
        proc = AsyncStreamingUpdateProcessor(config, store, ready, None)

    await proc.stop()

    assert fake_session.closed is True
    assert proc._owned_session is None


@pytest.mark.asyncio
async def test_injected_factory_leaves_session_unowned():
    """When a factory is injected, no session is built and none is owned."""
    with mock.patch.object(async_streaming, "make_client_session") as make_session:
        proc, store, ready, factory = _make_processor([])

    make_session.assert_not_called()
    assert proc._owned_session is None

    # stop() must not attempt to close a session it doesn't own.
    await proc.stop()
    assert proc._owned_session is None


@pytest.mark.asyncio
async def test_diagnostics_recorded_on_successful_init():
    from ldclient.impl.events.diagnostics import _DiagnosticAccumulator

    diag = _DiagnosticAccumulator(1)
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [_start(), _event('put', put_data)]

    proc, store, ready, _ = await _run_with_actions(actions, diag=diag)

    recorded = diag.create_event_and_reset(0, 0)['streamInits']
    assert len(recorded) == 1
    assert recorded[0]['failed'] is False

    await proc.stop()
