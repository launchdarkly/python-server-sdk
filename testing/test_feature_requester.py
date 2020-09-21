import pytest

from ldclient.config import Config
from ldclient.feature_requester import FeatureRequesterImpl
from ldclient.util import UnsuccessfulResponseException
from ldclient.version import VERSION
from ldclient.versioned_data_kind import FEATURES, SEGMENTS
from testing.http_util import start_server, BasicResponse, JsonResponse
from testing.proxy_test_util import do_proxy_tests

def test_get_all_data_returns_data():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)
        
        flags = { 'flag1': { 'key': 'flag1' } }
        segments = { 'segment1': { 'key': 'segment1' } }
        resp_data = { 'flags': flags, 'segments': segments }
        expected_data = { FEATURES: flags, SEGMENTS: segments }
        server.for_path('/sdk/latest-all', JsonResponse(resp_data))

        result = fr.get_all_data()
        assert result == expected_data

def test_get_all_data_sends_headers():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri)
        fr = FeatureRequesterImpl(config)

        resp_data = { 'flags': {}, 'segments': {} }
        server.for_path('/sdk/latest-all', JsonResponse(resp_data))

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
        server.for_path('/sdk/latest-all', JsonResponse(resp_data))

        fr.get_all_data()
        req = server.require_request()
        assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask/0.1.0'

def test_get_all_data_sends_wrapper_header_without_version():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', base_uri = server.uri,
                        wrapper_name = 'Flask')
        fr = FeatureRequesterImpl(config)

        resp_data = { 'flags': {}, 'segments': {} }
        server.for_path('/sdk/latest-all', JsonResponse(resp_data))

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
        server.for_path(req_path, JsonResponse(resp_data1, { 'Etag': etag1 }))

        result = fr.get_all_data()
        assert result == expected_data1
        req = server.require_request()
        assert 'If-None-Match' not in req.headers.keys()

        server.for_path(req_path, BasicResponse(304, None, { 'Etag': etag1 }))

        result = fr.get_all_data()
        assert result == expected_data1
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag1

        server.for_path(req_path, JsonResponse(resp_data2, { 'Etag': etag2 }))

        result = fr.get_all_data()
        assert result == expected_data2
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag1

        server.for_path(req_path, BasicResponse(304, None, { 'Etag': etag2 }))

        result = fr.get_all_data()
        assert result == expected_data2
        req = server.require_request()
        assert req.headers['If-None-Match'] == etag2

def test_http_proxy(monkeypatch):
    def _feature_requester_proxy_test(server, config, secure):
        resp_data = { 'flags': {}, 'segments': {} }
        expected_data = { FEATURES: {}, SEGMENTS: {} }
        server.for_path(config.base_uri + '/sdk/latest-all', JsonResponse(resp_data))
        fr = FeatureRequesterImpl(config)
        if secure:
            try:
                fr.get_all_data()
            except:
                pass # we expect this to fail because we don't have a real HTTPS proxy server
        else:
            result = fr.get_all_data()
            assert result == expected_data
    do_proxy_tests(_feature_requester_proxy_test, 'GET', monkeypatch)
