import threading
import time

import mock

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.datasource.status import DataSourceUpdateSinkImpl
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import UnsuccessfulResponseException
from ldclient.interfaces import (DataSourceErrorKind, DataSourceState,
                                 DataSourceStatus)
from ldclient.testing.builders import *
from ldclient.testing.stub_util import MockFeatureRequester, MockResponse
from ldclient.testing.test_util import SpyListener
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


def setup_processor(config):
    global pp
    pp = PollingUpdateProcessor(config, mock_requester, store, ready)
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


def test_http_401_error_causes_immediate_failure():
    verify_unrecoverable_http_error(401)


def test_http_403_error_causes_immediate_failure():
    verify_unrecoverable_http_error(401)


def test_http_408_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(408)


def test_http_429_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(429)


def test_http_500_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(500)


def test_http_503_error_does_not_cause_immediate_failure():
    verify_recoverable_http_error(503)


@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def verify_unrecoverable_http_error(http_status_code, ignore_mock):
    spy = SpyListener()
    listeners = Listeners()
    listeners.add(spy)

    config = Config("SDK_KEY")
    config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())

    mock_requester.exception = UnsuccessfulResponseException(http_status_code)
    setup_processor(config)
    finished = ready.wait(0.5)
    assert finished
    assert not pp.initialized()
    assert mock_requester.request_count == 1

    assert len(spy.statuses) == 1
    assert spy.statuses[0].state == DataSourceState.OFF
    assert spy.statuses[0].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert spy.statuses[0].error.status_code == http_status_code


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
