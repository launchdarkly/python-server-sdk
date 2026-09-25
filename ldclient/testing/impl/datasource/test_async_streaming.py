"""
Tests for AsyncStreamingUpdateProcessor.

These tests inject a mock SSE factory that yields pre-configured actions
rather than making real network connections.
"""

import asyncio
import json
import logging
import ssl
import time
from unittest import mock

import aiohttp
import pytest
from aiohttp.client_reqrep import ConnectionKey

from ldclient.config import Config
from ldclient.impl.datasource import async_streaming
from ldclient.impl.datasource.async_streaming import (
    AsyncStreamingUpdateProcessor
)
from ldclient.impl.model import ModelEntity
from ldclient.impl.retry import (
    EXTENDED_CEILING_DELAY,
    EXTENDED_INITIAL_DELAY,
    NORMAL_STREAMING_CEILING_DELAY,
    STREAMING_RESET_INTERVAL,
    AfterHealthyFor,
    RetryState,
    for_streaming
)
from ldclient.interfaces import DataSourceErrorKind, DataSourceState
from ldclient.testing.builders import FlagBuilder, SegmentBuilder
from ldclient.testing.mock_async_components import MockAsyncFeatureStore
from ldclient.testing.test_util import (
    no_retry_jitter,
    record_healthy_windows,
    ticking_clock
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


def _item_dict(item):
    """Convert a model entity (FeatureFlag, Segment, etc.) to a plain dict."""
    return item.to_json_dict() if isinstance(item, ModelEntity) else item


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


def _retry_state_with(policy: AfterHealthyFor) -> RetryState:
    """A retry state with tiny delays and a caller-supplied reset policy, so a
    test can watch the window."""
    return RetryState(
        normal_initial_delay=0.001,
        normal_ceiling_delay=0.001,
        extended_initial_delay=0.001,
        extended_ceiling_delay=0.001,
        reset_policy=policy,
    )


ONE_HOUR = 60 * 60


def _fast_retry_state(delay: float = 0.001) -> RetryState:
    """A retry state whose every delay is ``delay``: small enough to skip the
    real extended-regime wait, or large enough to prove a stop interrupts one."""
    return RetryState(
        normal_initial_delay=delay,
        normal_ceiling_delay=delay,
        extended_initial_delay=delay,
        extended_ceiling_delay=delay,
        reset_policy=AfterHealthyFor(STREAMING_RESET_INTERVAL),
    )


# aiohttp's connection errors read the connection key when they are turned
# into a string, which the data source does, so a real one is needed here.
_CONNECTION_KEY = ConnectionKey(
    host='stream.launchdarkly.com',
    port=443,
    is_ssl=True,
    ssl=True,
    proxy=None,
    proxy_auth=None,
    proxy_headers_hash=None,
    server_hostname=None,
)


def _zero_delay_retry_state() -> RetryState:
    """A retry state whose normal regime waits no time at all, so a test can
    drive ``_handle_error`` without a real sleep. The extended bounds stay
    real, so a misclassification still shows up in ``max_delay``."""
    return RetryState(
        normal_initial_delay=0,
        normal_ceiling_delay=NORMAL_STREAMING_CEILING_DELAY,
        extended_initial_delay=EXTENDED_INITIAL_DELAY,
        extended_ceiling_delay=EXTENDED_CEILING_DELAY,
        reset_policy=AfterHealthyFor(STREAMING_RESET_INTERVAL),
    )


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
        self.sdk_managed_retry: list = []

    def create(self, url: str, initial_retry_delay: float, sdk_managed_retry: bool = False) -> _MockSSE:
        sse = _MockSSE(self._actions)
        self.created.append(sse)
        self.sdk_managed_retry.append(sdk_managed_retry)
        return sse


def _make_processor(actions, config=None, store=None, ready_event=None, diag=None, retry_state=None):
    config = config or _make_config()
    store = store or MockAsyncFeatureStore()
    ready_event = ready_event or asyncio.Event()
    factory = _MockSSEFactory(actions)
    proc = AsyncStreamingUpdateProcessor(config, store, ready_event, diag, factory, retry_state=retry_state)
    return proc, store, ready_event, factory


async def _run_with_actions(actions: list, config=None, store=None, ready_event=None,
                            diag=None, extra_ready_timeout=3.0, retry_state=None):
    """Run the processor against a fake SSE action sequence.

    Starts the processor and waits for the ready event (up to
    *extra_ready_timeout* seconds), then returns
    ``(processor, store, ready_event, factory)``.
    """
    proc, store, ready, factory = _make_processor(actions, config, store, ready_event, diag, retry_state)
    proc.start()
    try:
        await asyncio.wait_for(ready.wait(), timeout=extra_ready_timeout)
    except asyncio.TimeoutError:
        pass  # some tests expect ready NOT to be set
    return proc, store, ready, factory


async def _wait_until(pred, timeout=2.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not pred():
        assert loop.time() < deadline, "condition was not met in time"
        await asyncio.sleep(0.005)


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
async def test_server_close_backs_off_and_does_not_stop_the_processor():
    """A Fault with error=None is the server closing a connection it normally
    leaves open. The SDK backs off rather than reconnecting at once, but the
    processor keeps running."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [
        _start(),
        _event('put', put_data),
        _fault(error=None),  # clean close by the server
    ]

    retry = _fast_retry_state()
    proc, store, ready, factory = _make_processor(actions, retry_state=retry)
    proc.start()
    await asyncio.wait_for(ready.wait(), timeout=3.0)
    await _wait_until(lambda: retry._attempts >= 1)

    assert store.initialized
    assert not factory.created[0].closed
    assert not retry._extended

    await proc.stop()


@pytest.mark.asyncio
async def test_server_close_reports_a_network_error():
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

    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [_start(), _event('put', put_data), _fault(error=None)]

    proc, store, ready, _ = _make_processor(actions, config=config, store=store, retry_state=_fast_retry_state())
    proc.start()
    await _wait_until(lambda: any(s.state == DataSourceState.INTERRUPTED for s in statuses))

    interrupted = [s for s in statuses if s.state == DataSourceState.INTERRUPTED]
    assert interrupted[0].error is not None
    assert interrupted[0].error.kind == DataSourceErrorKind.NETWORK_ERROR

    await proc.stop()


@pytest.mark.asyncio
async def test_repeated_server_closes_stay_on_the_normal_curve():
    """A load balancer draining during a rolling deploy closes streams
    cleanly, over and over. That must never reach the extended regime."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = []
    for _ in range(10):
        actions += [_start(), _event('put', put_data), _fault(error=None)]

    retry = _fast_retry_state()
    proc, store, ready, _ = _make_processor(actions, retry_state=retry)
    proc.start()
    await _wait_until(lambda: retry._attempts >= 10, timeout=5.0)

    assert not retry._extended
    assert retry._max_delay == _fast_retry_state()._max_delay

    await proc.stop()


@pytest.mark.asyncio
async def test_our_own_interrupt_is_not_counted_as_a_server_close():
    """Bad JSON makes the SDK drop the connection itself. The SSE client then
    reports that close as a Fault with no error, and counting it would record
    the same failure twice and wait twice."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [
        _start(),
        _event('put', put_data),
        _event('patch', 'not valid json'),
        _fault(error=None),  # the close our own interrupt caused
    ]

    retry = _fast_retry_state()
    proc, store, ready, _ = _make_processor(actions, retry_state=retry)
    proc.start()
    await _wait_until(lambda: retry._attempts >= 1)
    await asyncio.sleep(0.1)

    assert retry._attempts == 1

    await proc.stop()


@pytest.mark.asyncio
async def test_a_leaked_interrupt_flag_does_not_swallow_a_server_close():
    """interrupt() is a no-op when the connection has already gone, so no
    Fault arrives to clear the flag. A new connection must clear it, or the
    next genuine close is recorded as ours and the backoff is skipped."""
    put_data = _make_put_data()
    actions = [
        _start(),
        _event('put', put_data),
        _fault(error=None),  # a close the SDK did not ask for
    ]

    retry = _fast_retry_state()
    proc, _, _, _ = _make_processor(actions, retry_state=retry)
    proc._interrupted_by_sdk = True
    proc.start()
    await _wait_until(lambda: retry._attempts >= 1)
    await asyncio.sleep(0.1)

    assert retry._attempts == 1

    await proc.stop()


@pytest.mark.asyncio
async def test_unexpected_http_error_keeps_the_processor_running():
    """A rejected SDK key is retried like any other failure. The state never
    goes OFF, and initialization is not falsely unblocked."""
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

    proc, store, ready, factory = await _run_with_actions(
        actions, config=config, store=store, extra_ready_timeout=0.2,
        retry_state=_fast_retry_state(),
    )

    assert not ready.is_set()
    assert not proc.initialized()
    assert not factory.created[0].closed
    assert all(s.state != DataSourceState.OFF for s in statuses)
    assert any(
        s.error is not None
        and s.error.kind == DataSourceErrorKind.ERROR_RESPONSE
        and s.error.status_code == 401
        for s in statuses
    )

    await proc.stop()


@pytest.mark.asyncio
async def test_unexpected_http_error_moves_to_the_extended_regime():
    from ld_eventsource.errors import HTTPStatusError

    actions = [_start(), _fault(error=HTTPStatusError(401))]
    retry = _fast_retry_state()
    proc, store, ready, _ = _make_processor(actions, retry_state=retry)
    proc.start()
    await _wait_until(lambda: retry._extended)

    await proc.stop()


@pytest.mark.asyncio
async def test_normal_http_error_stays_in_the_normal_regime():
    from ld_eventsource.errors import HTTPStatusError

    actions = [_start(), _fault(error=HTTPStatusError(503))]
    retry = _fast_retry_state()
    proc, store, ready, _ = _make_processor(actions, retry_state=retry)
    proc.start()
    await _wait_until(lambda: retry._attempts >= 1)

    assert not retry._extended

    await proc.stop()


@pytest.mark.parametrize(
    "error",
    [
        aiohttp.ClientConnectorCertificateError(
            _CONNECTION_KEY, ssl.SSLCertVerificationError("self-signed certificate")
        ),
        aiohttp.ClientConnectorSSLError(_CONNECTION_KEY, OSError("handshake failed")),
        ssl.SSLEOFError("EOF occurred in violation of protocol"),
        ConnectionResetError(104, "reset by peer"),
    ],
    ids=["aiohttp-certificate", "aiohttp-tls", "peer-close-handshake", "reset"],
)
@pytest.mark.asyncio
async def test_transport_failures_stay_in_the_normal_regime(error):
    """No transport failure reaches the extended regime, an aiohttp
    certificate failure included. Only an HTTP status can do that."""
    retry = _zero_delay_retry_state()
    proc, store, ready, _ = _make_processor([], retry_state=retry)
    proc._running = True

    # A misclassification would wait five minutes here, so bound the wait
    # rather than let the test hang.
    assert await asyncio.wait_for(proc._handle_error(error), timeout=2.0)

    assert not retry._extended
    assert retry._max_delay == NORMAL_STREAMING_CEILING_DELAY


class _NoSleep:
    """Stands in for the ``asyncio`` module inside async_streaming, so the wait
    in _handle_error returns at once. ``sleep`` is all that module uses."""

    def __init__(self):
        self.slept: list = []

    async def sleep(self, seconds):
        self.slept.append(seconds)


@pytest.mark.asyncio
async def test_the_log_reports_the_growing_retry_delay(caplog):
    """The message has to carry the real delay, so someone reading logs can see
    the backoff working. The vaguer wording it replaced could not show this."""
    from ld_eventsource.errors import HTTPStatusError

    caplog.set_level(logging.WARNING)
    no_sleep = _NoSleep()

    with no_retry_jitter(), mock.patch.object(async_streaming, 'asyncio', no_sleep):
        retry = for_streaming(1)
        proc, store, ready, _ = _make_processor([], retry_state=retry)
        proc._running = True

        await proc._handle_error(HTTPStatusError(401))
        await proc._handle_error(HTTPStatusError(401))

    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        "Received HTTP error 401 (invalid SDK key) for stream connection - will retry in 300.0s",
        "Received HTTP error 401 (invalid SDK key) for stream connection - will retry in 600.0s",
    ]
    # An error a person has to fix is logged at error level, every time.
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.ERROR]
    # The reported delay is the one actually waited.
    assert no_sleep.slept == [5 * 60, 10 * 60]


@pytest.mark.asyncio
async def test_the_processor_asks_the_factory_to_leave_the_delay_to_the_sdk():
    proc, store, ready, factory = _make_processor([])
    proc.start()
    await _wait_until(lambda: len(factory.created) > 0)

    assert factory.sdk_managed_retry == [True]

    await proc.stop()


@pytest.mark.asyncio
async def test_several_messages_on_one_stream_do_not_extend_the_reset_window():
    """The window starts at the first message and stays there, however many
    more arrive on the same stream."""
    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    patch_data = _make_patch_data(FEATURES, _item_dict(FlagBuilder('f1').version(2).build()))
    actions = [_start(), _event('put', put_data), _event('patch', patch_data)]

    policy = AfterHealthyFor(STREAMING_RESET_INTERVAL)
    retry = _retry_state_with(policy)
    # The clock moves on every read, so a window that had been restarted reads
    # back as a different time.
    windows = record_healthy_windows(policy)

    with ticking_clock():
        proc, store, ready, _ = _make_processor(actions, retry_state=retry)
        proc.start()
        await _wait_until(lambda: len(windows) >= 2)
        await proc.stop()

    assert len(set(windows)) == 1, "the window moved between messages"


@pytest.mark.asyncio
async def test_a_fresh_stream_starts_a_new_reset_window():
    """A stream teardown clears the window through record_failure, so the next
    stream measures its own stretch rather than inheriting the old one."""
    from ld_eventsource.errors import HTTPStatusError

    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    actions = [
        _start(),
        _event('put', put_data),
        _fault(error=HTTPStatusError(503)),
        _start(),
        _event('put', put_data),
    ]

    policy = AfterHealthyFor(STREAMING_RESET_INTERVAL)
    retry = _retry_state_with(policy)
    windows = record_healthy_windows(policy)

    with ticking_clock():
        proc, store, ready, _ = _make_processor(actions, retry_state=retry)
        proc.start()
        # One put per stream, so two signals in all.
        await _wait_until(lambda: len(windows) >= 2)
        await proc.stop()

    assert len(set(windows)) == 2, "the second stream reused the first window"


@pytest.mark.asyncio
async def test_an_extended_regime_wait_is_cut_short_by_stop():
    """Shutdown must not sit through an hour-long backoff. The healthy-stop case
    is test_stop_closes_sse_and_finishes_task; this one stops mid-wait."""
    from ld_eventsource.errors import HTTPStatusError

    retry = _fast_retry_state(ONE_HOUR)
    proc, store, ready, _ = _make_processor(
        [_start(), _fault(error=HTTPStatusError(401))], retry_state=retry
    )
    proc.start()
    # Confirm the wait under test really is long before measuring the stop.
    await _wait_until(lambda: retry.next_delay > 60)

    started = time.time()
    await proc.stop()
    elapsed = time.time() - started

    assert elapsed < 2, "stop() took %.2fs" % elapsed
    leaked = [t for t in proc._runner._tasks if not t.done()]
    assert leaked == [], "stop() returned with the task still running: %r" % leaked


@pytest.mark.asyncio
async def test_stop_before_start_and_stop_twice_are_safe():
    proc, store, ready, _ = _make_processor([])

    await proc.stop()
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
async def test_second_start_is_a_no_op():
    """AsyncLDClient.start() is documented as an idempotent no-op, so nothing
    underneath it may raise on a repeat call."""
    actions = [_start()]
    proc, store, ready, factory = _make_processor(actions)
    proc.start()
    try:
        proc.start()
        await _wait_until(lambda: len(factory.created) > 0)
        assert len(factory.created) == 1
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


class _FakeSession:
    """Minimal stand-in for an aiohttp.ClientSession that records closure."""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_default_construction_builds_configured_session():
    """With no injected factory, a configured session is built (lazily, on the
    event loop in _run) via make_client_session and handed to the SSE factory."""
    config = _make_config()
    store = MockAsyncFeatureStore()
    ready = asyncio.Event()
    fake_session = _FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ) as make_session, mock.patch.object(
        async_streaming, "AsyncSSEFactory"
    ) as factory_cls:
        factory_cls.return_value.create.return_value = _MockSSE([])  # empty stream -> _run blocks
        proc = AsyncStreamingUpdateProcessor(config, store, ready, None)

        # Deferred: nothing is created until the task runs.
        assert proc._owned_session is None

        proc.start()
        await _wait_until(lambda: proc._owned_session is not None)

        assert make_session.call_args == mock.call(config)
        assert factory_cls.call_args.kwargs["session"] is fake_session
        assert proc._owned_session is fake_session

        await proc.stop()


@pytest.mark.asyncio
async def test_default_construction_session_closed_on_stop():
    """The SDK-created session is closed when the processor stops."""
    config = _make_config()
    store = MockAsyncFeatureStore()
    ready = asyncio.Event()
    fake_session = _FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ), mock.patch.object(async_streaming, "AsyncSSEFactory") as factory_cls:
        factory_cls.return_value.create.return_value = _MockSSE([])
        proc = AsyncStreamingUpdateProcessor(config, store, ready, None)
        proc.start()
        await _wait_until(lambda: proc._owned_session is not None)

        await proc.stop()

        assert fake_session.closed is True
        assert proc._owned_session is None


@pytest.mark.asyncio
async def test_default_construction_session_closed_when_run_fails():
    """If _run fails after building the session (e.g. create() raises), the
    SDK-created session is closed rather than leaked."""
    config = _make_config()
    store = MockAsyncFeatureStore()
    ready = asyncio.Event()
    fake_session = _FakeSession()

    with mock.patch.object(
        async_streaming, "make_client_session", return_value=fake_session
    ), mock.patch.object(async_streaming, "AsyncSSEFactory") as factory_cls:
        factory_cls.return_value.create.side_effect = RuntimeError("boom")
        proc = AsyncStreamingUpdateProcessor(config, store, ready, None)
        proc.start()

        # _run builds the session, then create() raises. The finally must still
        # close the SDK-created session instead of leaking it.
        await _wait_until(lambda: fake_session.closed)
        assert proc._owned_session is None


@pytest.mark.asyncio
async def test_injected_factory_leaves_session_unowned():
    """When a factory is injected, no session is built and none is owned."""
    with mock.patch.object(async_streaming, "make_client_session") as make_session:
        proc, store, ready, factory = _make_processor([])
        proc.start()
        await asyncio.sleep(0.02)  # let _run start against the injected factory

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


@pytest.mark.asyncio
async def test_off_is_reported_before_teardown():
    """A slow close must not hold back the status that tells a waiter to give
    up, so OFF goes out before the connection and session are torn down."""
    order = []

    class _OrderingSink:
        async def init(self, all_data):
            pass

        def update_status(self, new_state, new_error):
            order.append(new_state)

    config = _make_config()
    config._data_source_update_sink = _OrderingSink()

    flag = FlagBuilder('f1').version(1).build()
    put_data = _make_put_data(flags={'f1': _item_dict(flag)})
    proc, _, _, factory = await _run_with_actions([_start(), _event('put', put_data)], config=config)

    sse = factory.created[0]
    real_close = sse.close

    async def close():
        order.append('closed')
        await real_close()

    sse.close = close

    await proc.stop()

    assert DataSourceState.OFF in order
    assert order.index(DataSourceState.OFF) < order.index('closed')
