"""
End-to-end tests of the sync client on the FDv2 data system, run against a
mock HTTP server.
"""

import json
import os
import tempfile
import time

import pytest

from ldclient import datasystem
from ldclient.client import Context, LDClient
from ldclient.config import Config, HTTPConfig
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.testing.http_util import (
    BasicResponse,
    CauseNetworkError,
    ChunkedResponse,
    JsonResponse,
    SequentialHandler,
    start_secure_server,
    start_server
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

sdk_key = 'sdk-key'
user = Context.from_dict({'key': 'userkey', 'kind': 'user'})
always_true_flag = {'key': 'flagkey', 'version': 1, 'on': False, 'offVariation': 1, 'variations': [False, True]}
selfsigned_cert = './ldclient/testing/selfsigned.pem'

# An unreachable host. The proxy tests rely on the proxy, not this host, to answer.
not_real_uri = 'http://not-real'


def fdv2_events(flags):
    """Return the FDv2 events for a full transfer of the given flags."""
    events = [{
        'event': 'server-intent',
        'data': {'payloads': [{'id': 'p1', 'target': 1, 'intentCode': 'xfer-full', 'reason': 'payload-missing'}]},
    }]
    for flag in flags:
        events.append({
            'event': 'put-object',
            'data': {'kind': 'flag', 'key': flag['key'], 'version': flag['version'], 'object': flag},
        })
    events.append({
        'event': 'payload-transferred',
        'data': {'state': '(p:p1:1)', 'id': 'p1', 'version': 1},
    })
    return events


def poll_response(flags):
    """Return a poll response that carries a full transfer of the given flags."""
    return JsonResponse({'events': fdv2_events(flags)})


def stream_response(flags):
    """Return an open SSE stream that sends a full transfer of the given flags."""
    stream = ChunkedResponse({'Content-Type': 'text/event-stream'})
    for e in fdv2_events(flags):
        stream.push('event: %s\ndata: %s\n\n' % (e['event'], json.dumps(e['data'])))
    return stream


def make_config(uri, builder, **kwargs):
    return Config(
        sdk_key,
        base_uri=uri,
        stream_uri=uri,
        send_events=False,
        datasystem_config=builder.build(),
        **kwargs,
    )


def test_client_starts_in_streaming_mode():
    with start_server() as server:
        with stream_response([always_true_flag]) as stream:
            server.for_path('/sdk/stream', stream)

            with LDClient(config=make_config(server.uri, datasystem.streaming())) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) is True

                r = server.await_request()
                assert r.path == '/sdk/stream'
                assert r.headers['Authorization'] == sdk_key


def test_client_starts_in_polling_mode():
    with start_server() as server:
        server.for_path('/sdk/poll', poll_response([always_true_flag]))

        with LDClient(config=make_config(server.uri, datasystem.polling())) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is True

            r = server.await_request()
            assert r.path == '/sdk/poll'
            assert r.headers['Authorization'] == sdk_key


def test_default_mode_polls_then_streams():
    with start_server() as server:
        with stream_response([always_true_flag]) as stream:
            server.for_path('/sdk/poll', poll_response([always_true_flag]))
            server.for_path('/sdk/stream', stream)

            with LDClient(config=make_config(server.uri, datasystem.default())) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) is True

                first = server.await_request()
                assert first.path == '/sdk/poll'
                assert first.headers['Authorization'] == sdk_key

                # The stream carries the basis from the poll as a query parameter.
                second = server.await_request()
                assert second.path.startswith('/sdk/stream')
                assert second.headers['Authorization'] == sdk_key


def test_client_retries_stream_after_recoverable_error():
    with start_server() as server:
        with stream_response([always_true_flag]) as stream:
            server.for_path('/sdk/stream', SequentialHandler(BasicResponse(503), stream))
            config = make_config(server.uri, datasystem.streaming(), initial_reconnect_delay=0.001)

            with LDClient(config=config) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) is True

                assert server.await_request().path == '/sdk/stream'
                assert server.await_request().path == '/sdk/stream'


def test_client_retries_poll_after_network_error(monkeypatch):
    # Lower the minimum poll interval so that the retry happens before the start wait ends.
    monkeypatch.setattr('ldclient.impl.datasourcev2.polling.DEFAULT_POLL_INTERVAL', 0.05)
    with start_server() as server:
        # The requester retries once per poll, so two errors fail the whole first poll.
        server.for_path('/sdk/poll', SequentialHandler(
            CauseNetworkError(), CauseNetworkError(), CauseNetworkError(), poll_response([always_true_flag])))
        builder = datasystem.custom().synchronizers(datasystem.polling_ds_builder().poll_interval(0.05))

        with LDClient(config=make_config(server.uri, builder)) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is True


def test_client_fails_fast_in_streaming_mode_with_401_error():
    with start_server() as server:
        server.for_path('/sdk/stream', BasicResponse(401))

        start_wait = 10
        started = time.monotonic()
        with LDClient(config=make_config(server.uri, datasystem.streaming()), start_wait=start_wait) as client:
            # A 401 turns the data source off, which releases the constructor early.
            assert time.monotonic() - started < start_wait / 2
            assert not client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is False


def test_client_fails_fast_in_polling_mode_with_401_error():
    with start_server() as server:
        server.for_path('/sdk/poll', BasicResponse(401))

        start_wait = 10
        started = time.monotonic()
        with LDClient(config=make_config(server.uri, datasystem.polling()), start_wait=start_wait) as client:
            assert time.monotonic() - started < start_wait / 2
            assert not client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is False


def test_cannot_connect_with_selfsigned_cert_by_default():
    with start_secure_server() as server:
        server.for_path('/sdk/poll', poll_response([]))

        with LDClient(config=make_config(server.uri, datasystem.polling()), start_wait=1) as client:
            assert not client.is_initialized()


def test_can_connect_with_selfsigned_cert_if_disable_ssl_verification_is_true():
    with start_secure_server() as server:
        server.for_path('/sdk/poll', poll_response([]))
        config = make_config(server.uri, datasystem.polling(), http=HTTPConfig(disable_ssl_verification=True))

        with LDClient(config=config) as client:
            assert client.is_initialized()


def test_can_connect_with_selfsigned_cert_by_setting_ca_certs():
    with start_secure_server() as server:
        server.for_path('/sdk/poll', poll_response([]))
        config = make_config(server.uri, datasystem.polling(), http=HTTPConfig(ca_certs=selfsigned_cert))

        with LDClient(config=config) as client:
            assert client.is_initialized()


def test_can_stream_with_selfsigned_cert_if_disable_ssl_verification_is_true():
    with start_secure_server() as server:
        with stream_response([always_true_flag]) as stream:
            server.for_path('/sdk/stream', stream)
            config = make_config(server.uri, datasystem.streaming(), http=HTTPConfig(disable_ssl_verification=True))

            with LDClient(config=config) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) is True


def test_polling_goes_through_http_proxy():
    with start_server() as proxy:
        # A proxied request names the absolute target URI as its path.
        proxy.for_path(not_real_uri + '/sdk/poll', poll_response([always_true_flag]))
        config = make_config(not_real_uri, datasystem.polling(), http=HTTPConfig(http_proxy=proxy.uri))

        with LDClient(config=config) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is True

            r = proxy.await_request()
            assert r.method == 'GET'
            assert r.path == not_real_uri + '/sdk/poll'


def test_streaming_goes_through_http_proxy():
    with start_server() as proxy:
        with stream_response([always_true_flag]) as stream:
            proxy.for_path(not_real_uri + '/sdk/stream', stream)
            config = make_config(not_real_uri, datasystem.streaming(), http=HTTPConfig(http_proxy=proxy.uri))

            with LDClient(config=config) as client:
                assert client.is_initialized()
                assert client.variation(always_true_flag['key'], user, False) is True

                r = proxy.await_request()
                assert r.method == 'GET'
                assert r.path == not_real_uri + '/sdk/stream'


def test_proxy_receives_proxy_authorization_header():
    with start_server() as proxy:
        proxy.for_path(not_real_uri + '/sdk/poll', poll_response([]))
        proxy_uri = proxy.uri.replace('http://', 'http://user:pass@')
        config = make_config(not_real_uri, datasystem.polling(), http=HTTPConfig(http_proxy=proxy_uri))

        with LDClient(config=config) as client:
            assert client.is_initialized()

            r = proxy.await_request()
            assert r.headers.get('Proxy-Authorization') == 'Basic dXNlcjpwYXNz'


def test_https_request_tunnels_through_http_proxy():
    with start_server() as proxy:
        # The mock proxy cannot complete a tunnel, so the test only checks the CONNECT.
        config = make_config('https://not-real', datasystem.polling(), http=HTTPConfig(http_proxy=proxy.uri))

        with LDClient(config=config, start_wait=0):
            assert proxy.await_request().method == 'CONNECT'


def test_offline_mode_makes_no_requests_and_returns_defaults():
    with start_server() as server:
        server.for_path('/sdk/poll', poll_response([always_true_flag]))
        server.for_path('/sdk/stream', BasicResponse(401))
        config = make_config(server.uri, datasystem.default(), offline=True)

        with LDClient(config=config) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is False

        assert server.requests.empty()


def test_daemon_mode_evaluates_from_store_and_makes_no_requests():
    store = InMemoryFeatureStore()
    store.init({FEATURES: {always_true_flag['key']: always_true_flag}, SEGMENTS: {}})

    with start_server() as server:
        server.for_path('/sdk/poll', poll_response([]))
        server.for_path('/sdk/stream', BasicResponse(401))

        with LDClient(config=make_config(server.uri, datasystem.daemon(store))) as client:
            assert client.is_initialized()
            assert client.variation(always_true_flag['key'], user, False) is True

        assert server.requests.empty()


file_flags_json = '''
  {
    "flags": {
      "flag1": {
        "key": "flag1",
        "on": true,
        "fallthrough": {
          "variation": 2
        },
        "variations": [ "fall", "off", "on" ]
      }
    },
    "flagValues": {
      "flag2": "value2"
    }
  }
'''


def make_temp_file(content):
    f, path = tempfile.mkstemp()
    os.write(f, content.encode('utf-8'))
    os.close(f)
    return path


def file_client(path):
    builder = datasystem.custom().synchronizers(datasystem.file_ds_builder([path]))
    return LDClient(config=Config(sdk_key, send_events=False, datasystem_config=builder.build()))


def test_evaluates_full_flag_from_file_data_source():
    path = make_temp_file(file_flags_json)
    try:
        with file_client(path) as client:
            assert client.is_initialized()
            assert client.variation('flag1', user, '') == 'on'
    finally:
        os.remove(path)


def test_evaluates_simplified_flag_from_file_data_source():
    path = make_temp_file(file_flags_json)
    try:
        with file_client(path) as client:
            assert client.variation('flag2', user, '') == 'value2'
    finally:
        os.remove(path)


def make_test_data_client(td):
    builder = datasystem.custom().synchronizers(td.builder)
    return LDClient(config=Config(sdk_key, send_events=False, datasystem_config=builder.build()))


def rule_flag(td):
    return td.flag(key='test-flag').fallthrough_variation(False).if_match('firstName', 'Mike').and_not_match('country', 'gb').then_return(True)


def test_evaluates_flag_from_test_data_v2():
    td = TestDataV2.data_source()
    td.update(rule_flag(td))

    with make_test_data_client(td) as client:
        # The first context matches the rule.
        user1 = Context.from_dict({'kind': 'user', 'key': 'user1', 'firstName': 'Mike', 'country': 'us'})
        eval1 = client.variation_detail('test-flag', user1, default='default')
        assert eval1.value is True
        assert eval1.variation_index == 0
        assert eval1.reason['kind'] == 'RULE_MATCH'

        # The second context fails the firstName clause and falls through.
        user2 = Context.from_dict({'kind': 'user', 'key': 'user2', 'firstName': 'Joe', 'country': 'us'})
        eval2 = client.variation_detail('test-flag', user2, default='default')
        assert eval2.value is False
        assert eval2.variation_index == 1
        assert eval2.reason['kind'] == 'FALLTHROUGH'


def test_evaluates_all_flags_from_test_data_v2():
    td = TestDataV2.data_source()
    td.update(rule_flag(td))

    with make_test_data_client(td) as client:
        user1 = Context.from_dict({'kind': 'user', 'key': 'user1', 'firstName': 'Mike', 'country': 'us'})
        flags_state = client.all_flags_state(user1, with_reasons=True)

        assert flags_state.valid
        assert flags_state.get_flag_value('test-flag') is True
        assert (flags_state.get_flag_reason('test-flag') or {}).get('kind') == 'RULE_MATCH'
