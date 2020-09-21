import json
import pytest
from threading import Event
import time

from ldclient.config import Config
from ldclient.diagnostics import _DiagnosticAccumulator
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.version import VERSION
from ldclient.versioned_data_kind import FEATURES, SEGMENTS
from testing.http_util import start_server, BasicResponse, CauseNetworkError, SequentialHandler
from testing.proxy_test_util import do_proxy_tests
from testing.stub_util import make_delete_event, make_patch_event, make_put_event, stream_content

brief_delay = 0.001

# These long timeouts are necessary because of a problem in the Windows CI environment where HTTP requests to
# the test server running at localhost tests are *extremely* slow. It looks like a similar issue to what's
# described at https://stackoverflow.com/questions/2617615/slow-python-http-server-on-localhost but we had no
# luck with the advice that was given there.
start_wait = 5
update_wait = 3

def test_request_properties():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.method == 'GET'
                assert req.headers.get('Authorization') == 'sdk-key'
                assert req.headers.get('User-Agent') == 'PythonClient/' + VERSION
                assert req.headers.get('X-LaunchDarkly-Wrapper') is None

def test_sends_wrapper_header():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri,
                            wrapper_name = 'Flask', wrapper_version = '0.1.0')
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
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri,
                            wrapper_name = 'Flask')
            server.for_path('/all', stream)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()
                req = server.await_request()
                assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask'

def test_receives_put_event():
    store = InMemoryFeatureStore()
    ready = Event()
    flag = { 'key': 'flagkey', 'version': 1 }
    segment = { 'key': 'segkey', 'version': 1 }

    with start_server() as server:
        with stream_content(make_put_event([ flag ], [ segment ])) as stream:
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
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
    flagv1 = { 'key': 'flagkey', 'version': 1 }
    flagv2 = { 'key': 'flagkey', 'version': 2 }
    segmentv1 = { 'key': 'segkey', 'version': 1 }
    segmentv2 = { 'key': 'segkey', 'version': 1 }

    with start_server() as server:
        with stream_content(make_put_event([ flagv1 ], [ segmentv1 ])) as stream:
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
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
    flagv1 = { 'key': 'flagkey', 'version': 1 }
    segmentv1 = { 'key': 'segkey', 'version': 1 }

    with start_server() as server:
        with stream_content(make_put_event([ flagv1 ], [ segmentv1 ])) as stream:
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
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
    flagv1 = { 'key': 'flagkey', 'version': 1 }
    flagv2 = { 'key': 'flagkey', 'version': 2 }

    with start_server() as server:
        with stream_content(make_put_event([ flagv1 ])) as stream1:
            with stream_content(make_put_event([ flagv2 ])) as stream2:
                config = Config(sdk_key = 'sdk-key', stream_uri = server.uri, initial_reconnect_delay = brief_delay)
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
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri, initial_reconnect_delay = brief_delay)
            server.for_path('/all', two_errors_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()                
                ready.wait(start_wait)
                assert sp.initialized()
                server.await_request
                server.await_request

@pytest.mark.parametrize("status", [ 400, 408, 429, 500, 503 ])
def test_recoverable_http_error(status):
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            two_errors_then_success = SequentialHandler(error_handler, error_handler, stream)
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri, initial_reconnect_delay = brief_delay)
            server.for_path('/all', two_errors_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()                
                ready.wait(start_wait)
                assert sp.initialized()
                server.should_have_requests(3)

@pytest.mark.parametrize("status", [ 401, 403, 404 ])
def test_unrecoverable_http_error(status):
    error_handler = BasicResponse(status)
    store = InMemoryFeatureStore()
    ready = Event()
    with start_server() as server:
        with stream_content(make_put_event()) as stream:
            error_then_success = SequentialHandler(error_handler, stream)
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri, initial_reconnect_delay = brief_delay)
            server.for_path('/all', error_then_success)

            with StreamingUpdateProcessor(config, store, ready, None) as sp:
                sp.start()                
                ready.wait(5)
                assert not sp.initialized()
                server.should_have_requests(1)

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
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
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
            config = Config(sdk_key = 'sdk-key', stream_uri = server.uri, initial_reconnect_delay = brief_delay)
            server.for_path('/all', error_then_success)
            diag_accum = _DiagnosticAccumulator(1)

            with StreamingUpdateProcessor(config, store, ready, diag_accum) as sp:
                sp.start()
                ready.wait(start_wait)
                recorded_inits = diag_accum.create_event_and_reset(0, 0)['streamInits']

                assert len(recorded_inits) == 2
                assert recorded_inits[0]['failed'] is True
                assert recorded_inits[1]['failed'] is False

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
