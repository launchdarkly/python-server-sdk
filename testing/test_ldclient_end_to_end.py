from ldclient.client import LDClient
from ldclient.config import Config, HTTPConfig
from testing.http_util import BasicResponse, SequentialHandler, start_secure_server, start_server
from testing.stub_util import make_put_event, poll_content, stream_content

import json
import pytest
import sys

sdk_key = 'sdk-key'
user = { 'key': 'userkey' }
always_true_flag = { 'key': 'flagkey', 'version': 1, 'on': False, 'offVariation': 1, 'variations': [ False, True ] }

def test_client_starts_in_streaming_mode():
    with start_server() as stream_server:
        with stream_content(make_put_event([ always_true_flag ])) as stream_handler:
            stream_server.for_path('/all', stream_handler)
            config = Config(sdk_key = sdk_key, stream_uri = stream_server.uri, send_events = False)

            with LDClient(config = config) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) == True

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key

def test_client_fails_to_start_in_streaming_mode_with_401_error():
    with start_server() as stream_server:
        stream_server.for_path('/all', BasicResponse(401))
        config = Config(sdk_key = sdk_key, stream_uri = stream_server.uri, send_events = False)

        with LDClient(config = config) as client:
            assert not client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) == False

def test_client_retries_connection_in_streaming_mode_with_non_fatal_error():
    with start_server() as stream_server:
        with stream_content(make_put_event([ always_true_flag ])) as stream_handler:
            error_then_success = SequentialHandler(BasicResponse(503), stream_handler)
            stream_server.for_path('/all', error_then_success)
            config = Config(sdk_key = sdk_key, stream_uri = stream_server.uri, initial_reconnect_delay = 0.001, send_events = False)

            with LDClient(config = config) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) == True

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key

def test_client_starts_in_polling_mode():
    with start_server() as poll_server:
        poll_server.for_path('/sdk/latest-all', poll_content([ always_true_flag ]))
        config = Config(sdk_key = sdk_key, base_uri = poll_server.uri, stream = False, send_events = False)

        with LDClient(config = config) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) == True

            r = poll_server.await_request()
            assert r.headers['Authorization'] == sdk_key

def test_client_fails_to_start_in_polling_mode_with_401_error():
    with start_server() as poll_server:
        poll_server.for_path('/sdk/latest-all', BasicResponse(401))
        config = Config(sdk_key = sdk_key, base_uri = poll_server.uri, stream = False, send_events = False)

        with LDClient(config = config) as client:
            assert not client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) == False

def test_client_sends_event_without_diagnostics():
    with start_server() as poll_server:
        with start_server() as events_server:
            poll_server.for_path('/sdk/latest-all', poll_content([ always_true_flag ]))
            events_server.for_path('/bulk', BasicResponse(202))

            config = Config(sdk_key = sdk_key, base_uri = poll_server.uri, events_uri = events_server.uri, stream = False,
                diagnostic_opt_out = True)
            with LDClient(config = config) as client:
                assert client.is_initialized()
                client.identify(user)
                client.flush()

                r = events_server.await_request()
                assert r.headers['Authorization'] == sdk_key
                data = json.loads(r.body)
                assert len(data) == 1
                assert data[0]['kind'] == 'identify'

def test_client_sends_diagnostics():
    with start_server() as poll_server:
        with start_server() as events_server:
            poll_server.for_path('/sdk/latest-all', poll_content([ always_true_flag ]))
            events_server.for_path('/diagnostic', BasicResponse(202))

            config = Config(sdk_key = sdk_key, base_uri = poll_server.uri, events_uri = events_server.uri, stream = False)
            with LDClient(config = config) as client:
                assert client.is_initialized()

                r = events_server.await_request()
                assert r.headers['Authorization'] == sdk_key
                data = json.loads(r.body)
                assert data['kind'] == 'diagnostic-init'

# The TLS tests are skipped in Python 3.3 because the embedded HTTPS server does not work correctly, causing
# a TLS handshake failure on the client side. It's unclear whether this is a problem with the self-signed
# certificate we are using or with some other server settings, but it does not appear to be a client-side
# problem.

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_cannot_connect_with_selfsigned_cert_by_default():
    with start_secure_server() as server:
        server.for_path('/sdk/latest-all', poll_content())
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False,
            send_events = False
        )
        with LDClient(config = config, start_wait = 1.5) as client:
            assert not client.is_initialized()

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_can_connect_with_selfsigned_cert_if_ssl_verify_is_false():
    with start_secure_server() as server:
        server.for_path('/sdk/latest-all', poll_content())
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False,
            send_events = False,
            http = HTTPConfig(disable_ssl_verification=True)
        )
        with LDClient(config = config) as client:
            assert client.is_initialized()

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_can_connect_with_selfsigned_cert_if_disable_ssl_verification_is_true():
    with start_secure_server() as server:
        server.for_path('/sdk/latest-all', poll_content())
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False,
            send_events = False,
            http = HTTPConfig(disable_ssl_verification = True)
        )
        with LDClient(config = config) as client:
            assert client.is_initialized()

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_can_connect_with_selfsigned_cert_by_setting_ca_certs():
    with start_secure_server() as server:
        server.for_path('/sdk/latest-all', poll_content())
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False,
            send_events = False,
            http = HTTPConfig(ca_certs = './testing/selfsigned.pem')
        )
        with LDClient(config = config) as client:
            assert client.is_initialized()
