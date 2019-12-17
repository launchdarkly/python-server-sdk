import pytest

from ldclient.config import Config
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.util import UnsuccessfulResponseException
from ldclient.version import VERSION
from ldclient.versioned_data_kind import FEATURES, SEGMENTS
from testing.http_util import start_server


def test_get_all_data_returns_data():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)
        
        flags = { 'flag1': { 'key': 'flag1' } }
        segments = { 'segment1': { 'key': 'segment1' } }
        resp_data = { 'flags': flags, 'segments': segments }
        expected_data = { FEATURES: flags, SEGMENTS: segments }
        server.setup_json_response('/sdk/latest-all', resp_data)

        result = fr.get_all_data()
        assert result == expected_data

def test_get_all_data_sends_headers():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)

        resp_data = { 'flags': {}, 'segments': {} }
        server.setup_json_response('/sdk/latest-all', resp_data)

        fr.get_all_data()
        req = server.require_request()
        assert req.headers['Authorization'] == 'sdk-key'
        assert req.headers['User-Agent'] == 'PythonClient/' + VERSION
        assert req.headers.get('X-LaunchDarkly-Wrapper') is None

def test_get_all_data_sends_wrapper_header():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri,
                        wrapper_name = 'Flask', wrapper_version = '0.1.0')
        fr = FeatureRequesterImpl(config)

        resp_data = { 'flags': {}, 'segments': {} }
        server.setup_json_response('/sdk/latest-all', resp_data)

        fr.get_all_data()
        req = server.require_request()
        assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask/0.1.0'

def test_get_all_data_sends_wrapper_header_without_version():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri,
                        wrapper_name = 'Flask')
        fr = FeatureRequesterImpl(config)

        resp_data = { 'flags': {}, 'segments': {} }
        server.setup_json_response('/sdk/latest-all', resp_data)

        fr.get_all_data()
        req = server.require_request()
        assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask'

def test_get_all_data_can_use_cached_data():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)

        etag1 = 'my-etag-1'
        etag2 = 'my-etag-2'
        resp_data1 = { 'flags': {}, 'segments': {} }
        resp_data2 = { 'flags': { 'flag1': { 'key': 'flag1' } }, 'segments': {} }
        expected_data1 = { FEATURES: {}, SEGMENTS: {} }
        expected_data2 = { FEATURES: { 'flag1': { 'key': 'flag1' } }, SEGMENTS: {} }
        req_path = '/sdk/latest-all'
        server.setup_json_response(req_path, resp_data1, { 'Etag': etag1 })

        result = fr.get_all_data()
        assert result == expected_data1
        req = server.require_request()
        assert 'If-None-Match' not in req.headers.keys()

        server.setup_response(req_path, 304, None, { 'Etag': etag1 })

        result = fr.get_all_data()
        assert result == expected_data1
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag1

        server.setup_json_response(req_path, resp_data2, { 'Etag': etag2 })

        result = fr.get_all_data()
        assert result == expected_data2
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag1

        server.setup_response(req_path, 304, None, { 'Etag': etag2 })

        result = fr.get_all_data()
        assert result == expected_data2
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag2

def test_get_one_flag_returns_data():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)
        key = 'flag1'
        flag_data = { 'key': key }
        server.setup_json_response('/sdk/latest-flags/' + key, flag_data)
        result = fr.get_one(FEATURES, key)
        assert result == flag_data

def test_get_one_flag_sends_headers():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)
        key = 'flag1'
        flag_data = { 'key': key }
        server.setup_json_response('/sdk/latest-flags/' + key, flag_data)
        fr.get_one(FEATURES, key)
        req = server.require_request()
        assert req.headers['Authorization'] == 'sdk-key'
        assert req.headers['User-Agent'] == 'PythonClient/' + VERSION
        assert req.headers.get('X-LaunchDarkly-Wrapper') is None

def test_get_one_flag_sends_wrapper_header():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri,
                        wrapper_name = 'Flask', wrapper_version = '0.1.0')
        fr = FeatureRequesterImpl(config)
        key = 'flag1'
        flag_data = { 'key': key }
        server.setup_json_response('/sdk/latest-flags/' + key, flag_data)
        fr.get_one(FEATURES, key)
        req = server.require_request()
        assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask/0.1.0'

def test_get_one_flag_sends_wrapper_header_without_version():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri,
                        wrapper_name = 'Flask')
        fr = FeatureRequesterImpl(config)
        key = 'flag1'
        flag_data = { 'key': key }
        server.setup_json_response('/sdk/latest-flags/' + key, flag_data)
        fr.get_one(FEATURES, key)
        req = server.require_request()
        assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask'

def test_get_one_flag_throws_on_error():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)
        with pytest.raises(UnsuccessfulResponseException) as e:
            fr.get_one(FEATURES, 'didnt-set-up-a-response-for-this-flag')
        assert e.value.status == 404

def test_get_one_flag_does_not_use_etags():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)

        etag = 'my-etag'
        key = 'flag1'
        flag_data = { 'key': key }
        req_path = '/sdk/latest-flags/' + key
        server.setup_json_response(req_path, flag_data, { 'Etag': etag })

        result = fr.get_one(FEATURES, key)
        assert result == flag_data
        req = server.require_request()
        assert 'If-None-Match' not in req.headers.keys()

        result = fr.get_one(FEATURES, key)
        assert result == flag_data
        req = server.require_request()
        assert 'If-None-Match' not in req.headers.keys() # did not send etag from previous request

def test_can_use_http_proxy_via_environment_var(monkeypatch):
    with start_server() as server:
        monkeypatch.setenv('http_proxy', server.uri)
        config = Config(sdk_key = 'sdk-key', base_uri = 'http://not-real')
        _verify_http_proxy_is_used(server, config)

def test_can_use_https_proxy_via_environment_var(monkeypatch):
    with start_server() as server:
        monkeypatch.setenv('https_proxy', server.uri)
        config = Config(sdk_key = 'sdk-key', base_uri = 'https://not-real')
        _verify_https_proxy_is_used(server, config)

def test_can_use_http_proxy_via_config():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = 'http://not-real', http_proxy = server.uri)
        _verify_http_proxy_is_used(server, config)

def test_can_use_https_proxy_via_config():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = 'https://not-real', http_proxy = server.uri)
        _verify_https_proxy_is_used(server, config)

def _verify_http_proxy_is_used(server, config):
    fr = FeatureRequesterImpl(config)

    resp_data = { 'flags': {}, 'segments': {} }
    expected_data = { FEATURES: {}, SEGMENTS: {} }
    server.setup_json_response(config.base_uri + '/sdk/latest-all', resp_data)

    # For an insecure proxy request, our stub server behaves enough like the real thing to satisfy the
    # HTTP client, so we should be able to see the request go through. Note that the URI path will
    # actually be an absolute URI for a proxy request.
    result = fr.get_all_data()
    assert result == expected_data
    req = server.require_request()
    assert req.method == 'GET'

def _verify_https_proxy_is_used(server, config):
    fr = FeatureRequesterImpl(config)

    resp_data = { 'flags': {}, 'segments': {} }
    server.setup_json_response(config.base_uri + '/sdk/latest-all', resp_data)

    # Our simple stub server implementation can't really do HTTPS proxying, so the request will fail, but
    # it can still record that it *got* the request, which proves that the request went to the proxy.
    try:
        fr.get_all_data()
    except:
        pass
    req = server.require_request()
    assert req.method == 'CONNECT'
