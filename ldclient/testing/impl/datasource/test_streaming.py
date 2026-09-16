import logging
import ssl
import time
from threading import Event
from typing import List

import pytest
from ld_eventsource import SSEClient
from ld_eventsource.actions import Fault
from ld_eventsource.config import (
    ConnectStrategy,
    ErrorStrategy,
    RetryDelayStrategy
)
from ld_eventsource.errors import HTTPStatusError

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.datasource_common import StreamClosedError
from ldclient.impl.datasource.status import DataSourceUpdateSinkImpl
from ldclient.impl.datasource.streaming import StreamingUpdateProcessor
from ldclient.impl.events.diagnostics import _DiagnosticAccumulator
from ldclient.impl.listeners import Listeners
from ldclient.impl.retry import (
    STREAMING_RESET_INTERVAL,
    AfterHealthyFor,
    RetryState,
    for_streaming
)
from ldclient.interfaces import (
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatus
)
from ldclient.testing.builders import *
from ldclient.testing.http_util import (
    BasicResponse,
    CauseNetworkError,
    SequentialHandler,
    start_server
)
from ldclient.testing.proxy_test_util import do_proxy_tests
from ldclient.testing.stub_util import (
    make_delete_event,
    make_invalid_put_event,
    make_patch_event,
    make_put_event,
    stream_content
)
from ldclient.testing.test_util import (
    SpyListener,
    no_retry_jitter,
    record_healthy_windows,
    ticking_clock
)
from ldclient.version import VERSION
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

brief_delay = 0.001


def fast_retry_state(delay=brief_delay):
    """A retry state with tiny delays, so a test does not have to wait out the
    real extended-regime delay of five minutes."""
    return RetryState(
        normal_initial_delay=delay,
        normal_ceiling=delay,
        extended_initial_delay=delay,
        extended_ceiling=delay,
        reset_policy=AfterHealthyFor(STREAMING_RESET_INTERVAL),
    )


# These long timeouts are necessary because of a problem in the Windows CI environment where HTTP requests to
# the test server running at localhost tests are *extremely* slow. It looks like a similar issue to what's
# described at https://stackoverflow.com/questions/2617615/slow-python-http-server-on-localhost but we had no
# luck with the advice that was given there.
start_wait = 10
update_wait = 3


def test_request_properties():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.method == 'GET'
                assert req.headers.get('Authorization') == 'sdk-key'
                assert req.headers.get('User-Agent') == 'PythonClient/' + VERSION
                assert req.headers.get('X-LaunchDarkly-Wrapper') is None
                assert req.headers.get('X-LaunchDarkly-Instance-Id') is None
                assert req.headers.get('X-LaunchDarkly-Tags') is None


def test_sends_instance_id():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, wrapper_name='Flask', wrapper_version='0.1.0')
            config._instance_id = 'my-instance-id'
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.headers.get('X-LaunchDarkly-Instance-Id') == 'my-instance-id'


def test_sends_wrapper_header():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, wrapper_name='Flask', wrapper_version='0.1.0')
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask/0.1.0'


def test_sends_wrapper_header_without_version():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, wrapper_name='Flask')
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask'


def test_sends_tag_header():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, application={"id": "my-id", "version": "my-version"})
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.headers.get('X-LaunchDarkly-Tags') == 'application-id/my-id application-version/my-version'


def test_receives_put_event():
    store = InMemoryFeatureStore()
    ready = Event()
    flag = FlagBuilder('flagkey').version(1).build()
    segment = SegmentBuilder('segkey').version(1).build()

    with start_server() as server:
        with stream_content(make_put_event([flag], [segment])) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                expect_item(store, FEATURES, flag)
                expect_item(store, SEGMENTS, segment)


def test_receives_patch_events():
    store = InMemoryFeatureStore()
    ready = Event()
    flagv1 = FlagBuilder('flagkey').version(1).build()
    flagv2 = FlagBuilder('flagkey').version(2).build()
    segmentv1 = SegmentBuilder('segkey').version(1).build()
    segmentv2 = SegmentBuilder('segkey').version(2).build()

    with start_server() as server:
        with stream_content(make_put_event([flagv1], [segmentv1])) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                expect_item(store, FEATURES, flagv1)
                expect_item(store, SEGMENTS, segmentv1)

                stream.push(make_patch_event(FEATURES, flagv2))
                expect_update(store, FEATURES, flagv2)

                stream.push(make_patch_event(SEGMENTS, segmentv2))
                expect_update(store, SEGMENTS, segmentv2)


def test_receives_delete_events():
    store = InMemoryFeatureStore()
    ready = Event()
    flagv1 = FlagBuilder('flagkey').version(1).build()
    segmentv1 = SegmentBuilder('segkey').version(1).build()

    with start_server() as server:
        with stream_content(make_put_event([flagv1], [segmentv1])) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                expect_item(store, FEATURES, flagv1)
                expect_item(store, SEGMENTS, segmentv1)

                stream.push(make_delete_event(FEATURES, flagv1['key'], 2))
                expect_delete(store, FEATURES, flagv1['key'])

                stream.push(make_delete_event(SEGMENTS, segmentv1['key'], 2))
                expect_delete(store, SEGMENTS, segmentv1['key'])


def test_reconnects_if_stream_is_broken():
    store = InMemoryFeatureStore()
    ready = Event()
    flagv1 = FlagBuilder('flagkey').version(1).build()
    flagv2 = FlagBuilder('flagkey').version(2).build()

    with start_server() as server:
        with stream_content(make_put_event([flagv1])) as stream1:
            with stream_content(make_put_event([flagv2])) as stream2:
                config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
                server.for_path('/all', SequentialHandler(stream1, stream2))

                with StreamingUpdateProcessor(config, store, ready, None) as sp:
                    sp.start()
                    server.await_request
                    ready.wait(start_wait)
                    assert sp.initialized()
                    expect_item(store, FEATURES, flagv1)

                    stream1.close()
                    server.await_request
                    expect_update(store, FEATURES, flagv2)


def test_retries_on_network_error():
    error_handler = CauseNetworkError()
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            two_errors_then_success = SequentialHandler(error_handler, error_handler, stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            server.for_path('/all', two_errors_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.await_request
                server.await_request


@pytest.mark.parametrize("status", [400, 408, 429, 500, 503])
def test_recoverable_http_error(status):
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            two_errors_then_success = SequentialHandler(error_handler, error_handler, stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            server.for_path('/all', two_errors_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(3)


@pytest.mark.parametrize("status", [401, 403, 404])
def test_unexpected_http_error_backs_off_a_long_way(status):
    """An error that needs a person to fix it does not stop the stream, but the
    next attempt is five minutes out, so only one request is made here."""
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            error_then_success = SequentialHandler(error_handler, stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            server.for_path('/all', error_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                # Initialization is not falsely unblocked: the caller waits out
                # its own start_wait and then finds the client uninitialized.
                assert not ready.wait(1)
                assert not sp.initialized()
                assert sp.is_alive()
                assert sp._retry._extended
                server.should_have_requests(1)


@pytest.mark.parametrize("status", [401, 403, 404])
def test_unexpected_http_error_keeps_retrying(status):
    """The same failure with the delay compressed: the stream recovers once the
    service does, rather than staying down for ever."""
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            error_then_success = SequentialHandler(error_handler, stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

            spy = SpyListener()
            listeners = Listeners()
            listeners.add(spy)
            config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
            server.for_path('/all', error_then_success)

            with StreamingUpdateProcessor(config, store, ready, None, retry_state=fast_retry_state()) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(2)

                assert all(s.state != DataSourceState.OFF for s in spy.statuses)
                assert spy.statuses[0].state == DataSourceState.INITIALIZING
                assert spy.statuses[0].error.status_code == status
                assert spy.statuses[-1].state == DataSourceState.VALID


def test_sse_client_hands_us_the_fault_before_it_waits():
    """Pins the ld_eventsource ordering the SDK relies on.

    The SDK computes and takes the retry delay itself, which only works
    because SSEClient yields the Fault to the caller before its next connect
    attempt sleeps. A library change that slept first would make this test
    time out rather than fail quietly.
    """
    with start_server() as server:
        server.for_path('/all', BasicResponse(503))
        client = SSEClient(
            connect=ConnectStrategy.http(url=server.uri + '/all'),
            error_strategy=ErrorStrategy.always_continue(),
            initial_retry_delay=30,
            retry_delay_strategy=RetryDelayStrategy.default(max_delay=30, backoff_multiplier=2),
            retry_delay_reset_threshold=0,
        )
        try:
            started = time.time()
            first = next(iter(client.all))
            elapsed = time.time() - started
        finally:
            client.close()

        assert isinstance(first, Fault)
        assert isinstance(first.error, HTTPStatusError)
        # The library has a long delay queued up but has not taken it yet.
        assert client.next_retry_delay >= 15
        assert elapsed < 5


def test_the_sdk_configures_the_sse_client_never_to_wait():
    """The SDK owns the delay, so the library's own delay must stay at zero
    however long the outage lasts."""
    store = InMemoryFeatureStore()
    with start_server() as server:
        server.for_path('/all', BasicResponse(503))
        config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=30)
        sp = StreamingUpdateProcessor(config, store, Event(), None)
        client = sp._create_sse_client()
        try:
            actions = iter(client.all)
            first = next(actions)
            second = next(actions)
        finally:
            client.close()

        assert isinstance(first, Fault)
        assert isinstance(second, Fault)
        assert client.next_retry_delay == 0


def test_server_close_backs_off_and_keeps_the_stream_running():
    """The service normally leaves the connection open, so a clean close is a
    connection failure: the SDK reports it and backs off, rather than
    reconnecting in a tight loop."""
    store = InMemoryFeatureStore()
    ready = Event()
    flagv1 = FlagBuilder('flagkey').version(1).build()
    flagv2 = FlagBuilder('flagkey').version(2).build()

    with start_server() as server:
        with stream_content(make_put_event([flagv1])) as stream1:
            with stream_content(make_put_event([flagv2])) as stream2:
                config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

                spy = SpyListener()
                listeners = Listeners()
                listeners.add(spy)
                config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
                server.for_path('/all', SequentialHandler(stream1, stream2))

                retry = fast_retry_state()
                with StreamingUpdateProcessor(config, store, ready, None, retry_state=retry) as sp:
                    sp.start()
                    ready.wait(start_wait)
                    assert sp.initialized()

                    stream1.close()
                    expect_update(store, FEATURES, flagv2)

                    assert retry._attempts >= 1
                    assert not retry._extended

                    interrupted = [s for s in spy.statuses if s.state == DataSourceState.INTERRUPTED]
                    assert len(interrupted) >= 1
                    assert interrupted[0].error.kind == DataSourceErrorKind.NETWORK_ERROR


def test_server_close_uses_the_normal_delay_curve():
    """A clean close is a NORMAL failure. Classifying it UNEXPECTED would put
    a routine load-balancer drain into the extended regime and take a fleet
    out of service for up to an hour."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key='sdk-key', initial_reconnect_delay=1)
    retry = for_streaming(1)
    sp = StreamingUpdateProcessor(config, store, Event(), None, retry_state=retry)
    sp._running = True
    sp._stop_event.set()  # so the wait returns at once

    delays = []
    for _ in range(8):
        sp._handle_error(StreamClosedError())
        delays.append(retry._max_delay)

    assert not retry._extended
    assert delays == [30] * 8


def test_our_own_interrupt_is_not_counted_as_a_server_close():
    """Bad JSON makes the SDK drop the connection itself. The SSE client then
    reports that close as a Fault with no error, and counting it would record
    the same failure twice and wait twice."""
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as valid_stream, stream_content(make_invalid_put_event()) as invalid_stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

            statuses: List[DataSourceStatus] = []
            listeners = Listeners()

            # The stream fixture holds the connection open, so it has to be
            # closed for the server to move on to the next handler. This
            # mirrors test_invalid_json_triggers_listener.
            def listener(s):
                if len(statuses) == 0:
                    invalid_stream.close()
                statuses.append(s)

            listeners.add(listener)

            config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
            server.for_path('/all', SequentialHandler(invalid_stream, valid_stream))

            retry = fast_retry_state()
            with StreamingUpdateProcessor(config, store, ready, None, retry_state=retry) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(2)

                # One failure for the bad JSON, not a second for the close it
                # caused.
                assert retry._attempts == 1


def _handle_errors_without_waiting(retry, errors):
    """Drives _handle_error for each error and returns nothing. The stop event
    is pre-set so the interruptible wait returns at once."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key='sdk-key', initial_reconnect_delay=1)
    sp = StreamingUpdateProcessor(config, store, Event(), None, retry_state=retry)
    sp._running = True
    sp._stop_event.set()
    for error in errors:
        sp._handle_error(error)


def test_the_log_reports_the_growing_retry_delay(caplog):
    """The message has to carry the real delay, so someone reading logs can see
    the backoff working. The vaguer wording it replaced could not show this."""
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_streaming(1)
        _handle_errors_without_waiting(retry, [HTTPStatusError(401), HTTPStatusError(401)])

    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        "Received HTTP error 401 (invalid SDK key) for stream connection - will retry in 300.0s",
        "Received HTTP error 401 (invalid SDK key) for stream connection - will retry in 600.0s",
    ]
    # An error a person has to fix is logged at error level, every time.
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.ERROR]


def test_a_normal_failure_logs_a_short_delay_at_warning_level(caplog):
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_streaming(1)
        _handle_errors_without_waiting(retry, [HTTPStatusError(503), HTTPStatusError(503)])

    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        "Received HTTP error 503 for stream connection - will retry in 1.0s",
        "Received HTTP error 503 for stream connection - will retry in 2.0s",
    ]
    assert [r.levelno for r in caplog.records] == [logging.WARNING, logging.WARNING]


def test_a_server_close_and_a_transport_error_both_report_a_delay(caplog):
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_streaming(1)
        _handle_errors_without_waiting(retry, [StreamClosedError(), ConnectionResetError(104, "reset by peer")])

    messages = [r.getMessage() for r in caplog.records]
    assert messages[0] == "The server closed the stream connection - will retry in 1.0s"
    assert messages[1] == "Error on stream connection: [Errno 104] reset by peer - will retry in 2.0s"


def test_several_messages_on_one_stream_do_not_extend_the_reset_window():
    """The window starts at the first message and stays there, however many
    more arrive on the same stream."""
    store = InMemoryFeatureStore()
    ready = Event()
    flag = FlagBuilder('flagkey').version(1).build()

    with start_server() as server:
        with stream_content(make_put_event([flag]) + make_patch_event(FEATURES, flag)) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            server.for_path('/all', stream)

            policy = AfterHealthyFor(STREAMING_RESET_INTERVAL)
            retry = RetryState(
                normal_initial_delay=brief_delay,
                normal_ceiling=brief_delay,
                extended_initial_delay=brief_delay,
                extended_ceiling=brief_delay,
                reset_policy=policy,
            )
            # The clock moves on every read, so a window that had been
            # restarted reads back as a different time.
            windows = record_healthy_windows(policy)
            with ticking_clock():
                with StreamingUpdateProcessor(config, store, ready, None, retry_state=retry) as sp:
                    sp.start()
                    ready.wait(start_wait)
                    assert sp.initialized()
                    expect_update(store, FEATURES, flag)

            assert len(windows) >= 2, "both messages should have signalled"
            assert len(set(windows)) == 1, "the window moved between messages"


def test_a_fresh_stream_starts_a_new_reset_window():
    """A stream teardown clears the window through record_failure, so the next
    stream measures its own stretch rather than inheriting the old one."""
    store = InMemoryFeatureStore()
    ready = Event()
    flagv1 = FlagBuilder('flagkey').version(1).build()
    flagv2 = FlagBuilder('flagkey').version(2).build()

    with start_server() as server:
        with stream_content(make_put_event([flagv1])) as stream1:
            with stream_content(make_put_event([flagv2])) as stream2:
                config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
                server.for_path('/all', SequentialHandler(stream1, stream2))

                policy = AfterHealthyFor(STREAMING_RESET_INTERVAL)
                retry = RetryState(
                    normal_initial_delay=brief_delay,
                    normal_ceiling=brief_delay,
                    extended_initial_delay=brief_delay,
                    extended_ceiling=brief_delay,
                    reset_policy=policy,
                )
                windows = record_healthy_windows(policy)
                with ticking_clock():
                    with StreamingUpdateProcessor(config, store, ready, None, retry_state=retry) as sp:
                        sp.start()
                        ready.wait(start_wait)
                        assert sp.initialized()

                        stream1.close()
                        expect_update(store, FEATURES, flagv2)

                assert len(set(windows)) == 2, "the second stream reused the first window"


@pytest.mark.parametrize(
    "error",
    [
        ssl.SSLCertVerificationError("unable to get local issuer certificate"),
        ssl.SSLEOFError("EOF occurred in violation of protocol"),
        ConnectionResetError(104, "reset by peer"),
    ],
    ids=["certificate", "peer-close-handshake", "reset"],
)
def test_transport_failures_stay_in_the_normal_regime(error):
    """No transport failure reaches the extended regime, a bad certificate
    included. Only an HTTP status can do that."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key='sdk-key', initial_reconnect_delay=1)
    retry = for_streaming(1)
    sp = StreamingUpdateProcessor(config, store, Event(), None, retry_state=retry)
    sp._running = True
    sp._stop_event.set()  # so the wait returns at once

    sp._handle_error(error)

    assert not retry._extended
    assert retry._max_delay == 30


def test_http_proxy(monkeypatch):
    def _stream_processor_proxy_test(server, config, secure):
        store = InMemoryFeatureStore()
        ready = Event()
        with stream_content(make_put_event()) as stream:
            server.for_path(config.stream_base_uri + '/all', stream)
            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                # Wait till the server has received a request. We need to do this even though do_proxy_tests also
                # does it, because if we return too soon out of this block, the object returned by stream_content
                # could be closed and the test server would no longer work.
                server.wait_until_request_received()
                if not secure:
                    # We only do this part with HTTP, because with HTTPS we don't have a real enough proxy server
                    # for the stream connection to work correctly - we can only detect the request.
                    ready.wait(start_wait)
                    assert sp.initialized()

    do_proxy_tests(_stream_processor_proxy_test, 'GET', monkeypatch)


def test_records_diagnostic_on_stream_init_success():
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            server.for_path('/all', stream)
            diag_accum = _DiagnosticAccumulator(1)

            with StreamingUpdateProcessor(config, store, ready, diag_accum) as sp:
                sp.start()
                ready.wait(start_wait)
                recorded_inits = diag_accum.create_event_and_reset(0, 0)['streamInits']

                assert len(recorded_inits) == 1
                assert recorded_inits[0]['failed'] is False


def test_records_diagnostic_on_stream_init_failure():
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            error_then_success = SequentialHandler(BasicResponse(503), stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            server.for_path('/all', error_then_success)
            diag_accum = _DiagnosticAccumulator(1)

            with StreamingUpdateProcessor(config, store, ready, diag_accum) as sp:
                sp.start()
                ready.wait(start_wait)
                recorded_inits = diag_accum.create_event_and_reset(0, 0)['streamInits']

                assert len(recorded_inits) == 2
                assert recorded_inits[0]['failed'] is True
                assert recorded_inits[1]['failed'] is False


@pytest.mark.parametrize("status", [400, 408, 429, 500, 503])
def test_status_includes_http_code(status):
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            two_errors_then_success = SequentialHandler(error_handler, error_handler, stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

            spy = SpyListener()
            listeners = Listeners()
            listeners.add(spy)

            config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
            server.for_path('/all', two_errors_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(3)

                assert len(spy.statuses) == 3

                assert spy.statuses[0].state == DataSourceState.INITIALIZING
                assert spy.statuses[0].error.kind == DataSourceErrorKind.ERROR_RESPONSE
                assert spy.statuses[0].error.status_code == status

                assert spy.statuses[1].state == DataSourceState.INITIALIZING
                assert spy.statuses[1].error.kind == DataSourceErrorKind.ERROR_RESPONSE
                assert spy.statuses[1].error.status_code == status

                assert spy.statuses[2].state == DataSourceState.VALID
                assert spy.statuses[2].error.kind == DataSourceErrorKind.ERROR_RESPONSE
                assert spy.statuses[2].error.status_code == status


def test_invalid_json_triggers_listener():
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as valid_stream, stream_content(make_invalid_put_event()) as invalid_stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

            statuses: List[DataSourceStatus] = []
            listeners = Listeners()

            def listener(s):
                if len(statuses) == 0:
                    invalid_stream.close()
                statuses.append(s)

            listeners.add(listener)

            config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
            server.for_path('/all', SequentialHandler(invalid_stream, valid_stream))

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(2)

                assert len(statuses) == 2

                assert statuses[0].state == DataSourceState.INITIALIZING
                assert statuses[0].error.kind == DataSourceErrorKind.INVALID_DATA
                assert statuses[0].error.status_code == 0

                assert statuses[1].state == DataSourceState.VALID


def test_failure_transitions_from_valid():
    """A rejected SDK key after the stream was valid reports INTERRUPTED. OFF
    is reserved for an explicit shutdown."""
    store = InMemoryFeatureStore()
    ready = Event()
    error_handler = BasicResponse(401)
    with start_server() as server:
        config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)

        spy = SpyListener()
        listeners = Listeners()
        listeners.add(spy)

        config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())

        # The sink has special handling for failures before the state is valid. So we manually set this to valid so we
        # can exercise the other branching logic within the sink.
        config.data_source_update_sink.update_status(DataSourceState.VALID, None)
        server.for_path('/all', error_handler)

        with StreamingUpdateProcessor(config, store, ready, None) as sp:
            sp.start()
            # The 401 is retried five minutes out, so readiness never fires.
            assert not ready.wait(1)
            server.should_have_requests(1)

            assert len(spy.statuses) == 2

            assert spy.statuses[0].state == DataSourceState.VALID

            assert spy.statuses[1].state == DataSourceState.INTERRUPTED
            assert spy.statuses[1].error.kind == DataSourceErrorKind.ERROR_RESPONSE
            assert spy.statuses[1].error.status_code == 401


def test_records_environment_id_from_stream_headers():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event(), headers={'X-LD-EnvID': 'env-abc-123'}) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            sink = DataSourceUpdateSinkImpl(store, Listeners(), Listeners())
            config._data_source_update_sink = sink
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sink.environment_id == 'env-abc-123'


def test_environment_id_is_none_when_not_provided():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key='sdk-key', stream_uri=server.uri)
            sink = DataSourceUpdateSinkImpl(store, Listeners(), Listeners())
            config._data_source_update_sink = sink
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sink.environment_id is None


def test_does_not_record_environment_id_from_error_response_headers():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            error_then_success = SequentialHandler(BasicResponse(503, None, {'X-LD-EnvID': 'env-from-error'}), stream)
            config = Config(sdk_key='sdk-key', stream_uri=server.uri, initial_reconnect_delay=brief_delay)
            sink = DataSourceUpdateSinkImpl(store, Listeners(), Listeners())
            config._data_source_update_sink = sink
            server.for_path('/all', error_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                ready.wait(start_wait)
                assert sink.environment_id is None


def expect_item(store, kind, item):
    assert store.get(kind, item['key'], lambda x: x) == item


def expect_update(store, kind, expected_item):
    await_item(store, kind, expected_item['key'], expected_item)


def expect_delete(store, kind, key):
    await_item(store, kind, key, None)


def await_item(store, kind, key, expected_item):
    deadline = time.time() + update_wait
    while time.time() < deadline:
        time.sleep(0.05)
        current_item = store.get(kind, key, lambda x: x)
        if current_item == expected_item:
            return
    assert False, 'expected %s = %s but value was still %s after %d seconds' % (key, json.dumps(expected_item), json.dumps(current_item), update_wait)
