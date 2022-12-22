import threading
import time
import mock

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.polling import PollingUpdateProcessor
from ldclient.impl.util import UnsuccessfulResponseException
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

from testing.builders import *
from testing.stub_util import MockFeatureRequester, MockResponse

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
    mock_requester.all_data = {
        FEATURES: {
            "flagkey": flag.to_json_dict()
        },
        SEGMENTS: {
            "segkey": segment.to_json_dict()
        }
    }
    setup_processor(Config("SDK_KEY"))
    ready.wait()
    assert store.get(FEATURES, "flagkey", lambda x: x) == flag
    assert store.get(SEGMENTS, "segkey", lambda x: x) == segment
    assert store.initialized
    assert pp.initialized()

# Note that we have to mock Config.poll_interval because Config won't let you set a value less than 30 seconds

@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def test_general_connection_error_does_not_cause_immediate_failure(ignore_mock):
    mock_requester.exception = Exception("bad")
    start_time = time.time()
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
def verify_unrecoverable_http_error(status, ignore_mock):
    mock_requester.exception = UnsuccessfulResponseException(status)
    setup_processor(Config("SDK_KEY"))
    finished = ready.wait(0.5)
    assert finished
    assert not pp.initialized()
    assert mock_requester.request_count == 1

@mock.patch('ldclient.config.Config.poll_interval', new_callable=mock.PropertyMock, return_value=0.1)
def verify_recoverable_http_error(status, ignore_mock):
    mock_requester.exception = UnsuccessfulResponseException(status)
    setup_processor(Config("SDK_KEY"))
    finished = ready.wait(0.5)
    assert not finished
    assert not pp.initialized()
    assert mock_requester.request_count >= 2
