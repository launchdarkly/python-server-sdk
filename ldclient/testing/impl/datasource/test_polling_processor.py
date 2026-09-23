import logging
import ssl
import threading
import time

import mock
import pytest

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.datasource.status import DataSourceUpdateSinkImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.retry import (
    POLLING_RESET_SUCCESSES,
    AfterConsecutiveSuccesses,
    RetryState,
    for_polling
)
from ldclient.impl.util import UnsuccessfulResponseException
from ldclient.interfaces import (
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatus
)
from ldclient.testing.builders import *
from ldclient.testing.stub_util import MockFeatureRequester, MockResponse
from ldclient.testing.sync_util import wait_until
from ldclient.testing.test_util import SpyListener, no_retry_jitter
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

pp = None
mock_requester = None
store = None
ready = None


def setup_function():
    global mock_requester, store, ready
    mock_requester = MockFeatureRequester()
    store = InMemoryFeatureStore()
    ready = threading.Event()


def teardown_function():
    if pp is not None:
        pp.stop()


ONE_HOUR = 60 * 60


def fast_retry_state(delay=0.05):
    """A retry state whose every delay is ``delay``: small enough to skip the
    real extended-regime wait, or large enough to prove a stop interrupts one."""
    return RetryState(
        normal_initial_delay=delay,
        normal_ceiling_delay=delay,
        extended_initial_delay=delay,
        extended_ceiling_delay=delay,
        reset_policy=AfterConsecutiveSuccesses(POLLING_RESET_SUCCESSES),
        operating_cadence=delay,
    )


def setup_processor(config, retry_state=None):
    global pp
    pp = PollingUpdateProcessor(config, mock_requester, store, ready, retry_state=retry_state)
    pp.start()


def test_successful_request_puts_feature_data_in_store():
    flag = FlagBuilder('flagkey').build()
    segment = SegmentBuilder('segkey').build()
    mock_requester.all_data = {FEATURES: {"flagkey": flag.to_json_dict()}, SEGMENTS: {"segkey": segment.to_json_dict()}}

    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
    setup_processor(config)
    ready.wait()
    assert store.get(FEATURES, "flagkey", lambda x: x) == flag
    assert store.get(SEGMENTS, "segkey", lambda x: x) == segment
    assert store.initialized
    assert pp.initialized()
    assert len(spy.statuses) == 1
    assert spy.statuses[0].state == DataSourceState.VALID
    assert spy.statuses[0].error is None


# Note that we have to mock Config.poll_interval because Config won't let you set a value less than 30 seconds


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def test_general_connection_error_does_not_cause_immediate_failure(ignore_mock):
    mock_requester.exception = Exception("bad")
    setup_processor(Config("SDK_KEY"))
    ready.wait(0.3)
    assert not pp.initialized()
    assert mock_requester.request_count >= 2


def test_http_401_error_does_not_stop_polling():
    verify_unexpected_http_error(401)


def test_http_403_error_does_not_stop_polling():
    verify_unexpected_http_error(403)


def test_http_404_error_does_not_stop_polling():
    verify_unexpected_http_error(404)


def test_http_408_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(408)


def test_http_429_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(429)


def test_http_500_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(500)


def test_http_503_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(503)


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def verify_unexpected_http_error(http_status_code, ignore_mock):
    """An error that needs a person to fix it -- a rejected SDK key, say -- is
    still retried. It must not stop the poller, must not report OFF, and must
    not falsely unblock initialization."""
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())

    mock_requester.exception = UnsuccessfulResponseException(http_status_code)
    setup_processor(config, retry_state=fast_retry_state())
    finished = ready.wait(0.5)
    assert not finished
    assert not pp.initialized()
    assert mock_requester.request_count >= 2

    assert len(spy.statuses) > 1
    for status in spy.statuses:
        assert status.state == DataSourceState.INITIALIZING
        assert status.error.kind == DataSourceErrorKind.ERROR_RESPONSE
        assert status.error.status_code == http_status_code


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def test_unexpected_http_error_moves_to_the_extended_regime(ignore_mock):
    retry = for_polling(0.1)
    mock_requester.exception = UnsuccessfulResponseException(401)
    setup_processor(Config("SDK_KEY"), retry_state=retry)

    # The extended regime starts at five minutes, so only the first poll runs.
    wait_until(lambda: retry.next_delay > 0.1)
    assert not ready.wait(0.1)
    assert mock_requester.request_count == 1


def test_the_first_success_after_an_outage_polls_at_the_cadence():
    # A backoff wait applies to a retry, not to every operation. The retry
    # state carries the wait, so this reads it there rather than measuring
    # elapsed time.
    with no_retry_jitter():
        retry = for_polling(30)
        processor = PollingUpdateProcessor(Config("SDK_KEY"), mock_requester, store, ready, retry_state=retry)

        mock_requester.exception = UnsuccessfulResponseException(401)
        processor._poll()
        assert retry.next_delay == 5 * 60

        mock_requester.exception = None
        mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
        processor._poll()
        assert retry.next_delay == 30
        assert retry._extended, "one success restores the cadence but does not reset"

        processor._poll()
        assert retry.next_delay == 30
        assert not retry._extended, "two successes in a row reset the state"


@pytest.mark.parametrize(
    "error",
    [
        ssl.SSLCertVerificationError("unable to get local issuer certificate"),
        ssl.SSLEOFError("EOF occurred in violation of protocol"),
        ConnectionResetError(104, "reset by peer"),
    ],
    ids=["certificate", "peer-close-handshake", "reset"],
)
def test_transport_failures_poll_again_at_the_cadence(error):
    """No transport failure reaches the extended regime, a bad certificate
    included. Only an HTTP status can do that."""
    retry = for_polling(30)
    processor = PollingUpdateProcessor(Config("SDK_KEY"), mock_requester, store, ready, retry_state=retry)

    mock_requester.exception = error
    processor._poll()
    assert retry.next_delay == 30
    assert not retry._extended


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.05)
def test_failure_transitions_from_valid(ignore_mock):
    """A rejected SDK key after a poll has succeeded reports INTERRUPTED. OFF
    is reserved for an explicit shutdown."""
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())

    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    setup_processor(config, retry_state=fast_retry_state())
    assert ready.wait(2)
    assert spy.statuses[0].state == DataSourceState.VALID

    mock_requester.exception = UnsuccessfulResponseException(401)
    deadline = time.time() + 2
    while spy.statuses[-1].state == DataSourceState.VALID and time.time() < deadline:
        time.sleep(0.01)

    assert spy.statuses[-1].state == DataSourceState.INTERRUPTED
    assert spy.statuses[-1].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert spy.statuses[-1].error.status_code == 401
    assert all(s.state != DataSourceState.OFF for s in spy.statuses)


def test_second_start_is_a_no_op():
    """A second start() must not raise. Thread.start() would, so the processor
    guards it."""
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    setup_processor(Config("SDK_KEY"))
    pp.start()

    assert ready.wait(2)
    assert pp.initialized()


def test_the_log_reports_the_growing_retry_delay(caplog):
    """The message has to carry the real delay, so someone reading logs can see
    the backoff working."""
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_polling(30)
        processor = PollingUpdateProcessor(Config("SDK_KEY"), mock_requester, store, ready, retry_state=retry)
        mock_requester.exception = UnsuccessfulResponseException(401)
        processor._poll()
        processor._poll()

    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        "Received HTTP error 401 (invalid SDK key) for polling request - will retry in 300.0s",
        "Received HTTP error 401 (invalid SDK key) for polling request - will retry in 600.0s",
    ]
    # An error a person has to fix is logged at error level, every time.
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.ERROR]


def test_a_normal_failure_logs_the_poll_interval_at_warning_level(caplog):
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_polling(30)
        processor = PollingUpdateProcessor(Config("SDK_KEY"), mock_requester, store, ready, retry_state=retry)
        mock_requester.exception = UnsuccessfulResponseException(503)
        processor._poll()

    record = caplog.records[0]
    assert record.getMessage() == "Received HTTP error 503 for polling request - will retry in 30.0s"
    assert record.levelno == logging.WARNING


def test_a_transport_error_reports_a_delay_and_keeps_its_stacktrace(caplog):
    caplog.set_level(logging.WARNING)

    with no_retry_jitter():
        retry = for_polling(30)
        processor = PollingUpdateProcessor(Config("SDK_KEY"), mock_requester, store, ready, retry_state=retry)
        mock_requester.exception = ConnectionResetError(104, "reset by peer")
        processor._poll()

    record = caplog.records[0]
    assert record.getMessage() == "Error encountered when updating flags: [Errno 104] reset by peer - will retry in 30.0s"
    # The handler has exited by the time this is logged, so the exception has to
    # be carried explicitly for the traceback to survive.
    assert record.exc_info is not None


def _polling_thread():
    """Finds the task's worker thread by name, so a test can prove it exited
    without reaching into the task's private state."""
    return next((t for t in threading.enumerate() if t.name == "ldclient.datasource.polling.repeating"), None)


def test_an_extended_regime_wait_is_cut_short_by_stop():
    """The reason the wait has to be interruptible at all. A 401 puts the next
    poll five minutes out, and shutdown must not sit through it."""
    mock_requester.exception = UnsuccessfulResponseException(401)
    retry = for_polling(30)
    setup_processor(Config("SDK_KEY"), retry_state=retry)

    # Let the first poll happen, so the task is inside the long wait.
    deadline = time.time() + 2
    while mock_requester.request_count < 1 and time.time() < deadline:
        time.sleep(0.01)
    assert mock_requester.request_count == 1
    assert retry._extended, "the wait under test should be minutes long"

    worker = _polling_thread()
    assert worker is not None

    started = time.time()
    pp.stop()
    worker.join(2)
    elapsed = time.time() - started

    # Without an interruptible wait this join would time out and the thread
    # would still be sitting in a 300-second sleep.
    assert not worker.is_alive()
    assert elapsed < 1


def test_an_absurd_poll_interval_does_not_kill_the_worker_thread():
    """``Event.wait`` raises above ``threading.TIMEOUT_MAX``, and that raise is
    outside the try block around the poll, so the thread used to die while the
    SDK still reported itself healthy."""
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    setup_processor(Config("SDK_KEY", poll_interval=1e10))

    assert ready.wait(2)
    worker = _polling_thread()
    assert worker is not None

    # A thread that raised on the wait exits as soon as the first poll returns.
    worker.join(0.3)
    assert worker.is_alive()


def test_stop_twice_and_stop_before_start_are_safe():
    """Neither a stop before the first poll nor a second stop should raise,
    including while an hour-long wait is pending."""
    mock_requester.exception = UnsuccessfulResponseException(401)
    processor = PollingUpdateProcessor(
        Config("SDK_KEY"), mock_requester, store, ready, retry_state=fast_retry_state(ONE_HOUR)
    )

    processor.stop()
    processor.stop()
    processor.start()
    processor.stop()


def test_stop_reports_off():
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    setup_processor(config)
    assert ready.wait(2)

    pp.stop()

    assert spy.statuses[-1].state == DataSourceState.OFF


def test_a_poll_finishing_after_stop_reports_nothing():
    """The poll still in flight when stop() ran must not report after OFF."""
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    processor = PollingUpdateProcessor(config, mock_requester, store, ready)

    processor.stop()
    processor._poll()

    assert [status.state for status in spy.statuses] == [DataSourceState.OFF]


def test_valid_status_is_reported_before_ready_is_set():
    # Mirrors go-server-sdk#442: a caller that wakes on readiness must not
    # still be able to read INITIALIZING.
    observed = []
    listeners = Listeners()
    listeners.add(lambda status: observed.append((status.state, ready.is_set())))

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}
    setup_processor(config)
    assert ready.wait(2)

    assert observed[0] == (DataSourceState.VALID, False)


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def verify_recoverable_http_error(http_status_code, ignore_mock):
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())

    mock_requester.exception = UnsuccessfulResponseException(http_status_code)
    setup_processor(config)
    finished = ready.wait(0.5)
    assert not finished
    assert not pp.initialized()
    assert mock_requester.request_count >= 2

    assert len(spy.statuses) > 1

    for status in spy.statuses:
        assert status.state == DataSourceState.INITIALIZING
        assert status.error.kind == DataSourceErrorKind.ERROR_RESPONSE
        assert status.error.status_code == http_status_code


class MockFeatureRequesterWithHeaders(MockFeatureRequester):
    def __init__(self, headers):
        super().__init__()
        self.headers = headers

    def get_all_data_with_headers(self):
        return (self.get_all_data(), self.headers)


def test_records_environment_id_from_polling_headers():
    global mock_requester
    mock_requester = MockFeatureRequesterWithHeaders({'X-LD-EnvID': 'env-abc-123'})
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}

    config = Config("SDK_KEY")
    sink = DataSourceUpdateSinkImpl(store, Listeners(), Listeners())
    config._data_source_update_sink = sink
    setup_processor(config)
    assert ready.wait(2)

    assert sink.environment_id == 'env-abc-123'


def test_environment_id_is_none_when_requester_provides_no_headers():
    mock_requester.all_data = {FEATURES: {}, SEGMENTS: {}}

    config = Config("SDK_KEY")
    sink = DataSourceUpdateSinkImpl(store, Listeners(), Listeners())
    config._data_source_update_sink = sink
    setup_processor(config)
    assert ready.wait(2)

    assert sink.environment_id is None
