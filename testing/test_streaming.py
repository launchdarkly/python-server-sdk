from threading import Event

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.streaming import StreamingUpdateProcessor
from ldclient.version import VERSION
from testing.http_util import start_server


fake_event = 'event:put\ndata: {"data":{"flags":{},"segments":{}}}\n\n'

# Note that our simple HTTP stub server implementation does not actually do streaming responses, so
# in these tests the connection will get closed after the response, causing the streaming processor
# to reconnect. For the purposes of the current tests, that's OK because we only care that the initial
# request and response were handled correctly.

def test_uses_stream_uri():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
        server.setup_response('/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })

        with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
            sp.start()
            req = server.await_request()
            assert req.method == 'GET'
            ready.wait(1)
            assert sp.initialized()

def test_sends_headers():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = server.uri)
        server.setup_response('/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })

        with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
            sp.start()
            req = server.await_request()
            assert req.headers.get('Authorization') == 'sdk-key'
            assert req.headers.get('User-Agent') == 'PythonClient/' + VERSION
            assert req.headers.get('X-LaunchDarkly-Wrapper') is None

def test_sends_wrapper_header():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = server.uri,
                        wrapper_name = 'Flask', wrapper_version = '0.1.0')
        server.setup_response('/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })

        with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
            sp.start()
            req = server.await_request()
            assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask/0.1.0'

def test_sends_wrapper_header_without_version():
    store = InMemoryFeatureStore()
    ready = Event()

    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = server.uri,
                        wrapper_name = 'Flask')
        server.setup_response('/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })

        with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
            sp.start()
            req = server.await_request()
            assert req.headers.get('X-LaunchDarkly-Wrapper') == 'Flask'

def test_can_use_http_proxy_via_environment_var(monkeypatch):
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = 'http://not-real')
        monkeypatch.setenv('http_proxy', server.uri)
        _verify_http_proxy_is_used(server, config)

def test_can_use_https_proxy_via_environment_var(monkeypatch):
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = 'https://not-real')
        monkeypatch.setenv('https_proxy', server.uri)
        _verify_https_proxy_is_used(server, config)

def test_can_use_http_proxy_via_config():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = 'http://not-real', http_proxy=server.uri)
        _verify_http_proxy_is_used(server, config)

def test_can_use_https_proxy_via_config():
    with start_server() as server:
        config = Config(sdk_key = 'sdk-key', stream_uri = 'https://not-real', http_proxy=server.uri)
        _verify_https_proxy_is_used(server, config)

def _verify_http_proxy_is_used(server, config):
    store = InMemoryFeatureStore()
    ready = Event()
    server.setup_response(config.stream_base_uri + '/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })
    with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
        sp.start()
        # For an insecure proxy request, our stub server behaves enough like the real thing to satisfy the
        # HTTP client, so we should be able to see the request go through. Note that the URI path will
        # actually be an absolute URI for a proxy request.
        req = server.await_request()
        assert req.method == 'GET'
        ready.wait(1)
        assert sp.initialized()

def _verify_https_proxy_is_used(server, config):
    store = InMemoryFeatureStore()
    ready = Event()
    server.setup_response(config.stream_base_uri + '/all', 200, fake_event, { 'Content-Type': 'text/event-stream' })
    with StreamingUpdateProcessor(config, None, store, ready, None) as sp:
        sp.start()
        # Our simple stub server implementation can't really do HTTPS proxying, so the request will fail, but
        # it can still record that it *got* the request, which proves that the request went to the proxy.
        req = server.await_request()
        assert req.method == 'CONNECT'
