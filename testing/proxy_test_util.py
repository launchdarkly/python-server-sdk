from ldclient.config import Config, HTTPConfig
from testing.http_util import start_server, BasicResponse, JsonResponse

# Runs tests of all of our supported proxy server configurations: secure or insecure, configured
# by Config.http_proxy or by an environment variable, with or without authentication. The action
# parameter is a function that takes three parameters: server, config, secure; the expectation is
# that it causes an HTTP/HTTPS request to be made via the configured proxy. The caller must pass
# in the monkeypatch fixture from pytest.
def do_proxy_tests(action, action_method, monkeypatch):
    # We'll test each permutation of use_env_vars, secure, and use_auth, except that if secure is
    # true then we'll only test with use_auth=false because we don't have a way to test proxy
    # authorization over HTTPS (even though we believe it works).
    for (use_env_vars, secure, use_auth) in [
        (False, False, False),
        (False, False, True),
        (False, True, False),
        (True, False, False),
        (True, False, True),
        (True, True, False)]:
        test_desc = "%s, %s, %s" % (
            "using env vars" if use_env_vars else "using Config",
            "secure" if secure else "insecure",
            "with auth" if use_auth else "no auth")
        with start_server() as server:
            proxy_uri = server.uri.replace('http://', 'http://user:pass@') if use_auth else server.uri
            target_uri = 'https://not-real' if secure else 'http://not-real'
            if use_env_vars:
                monkeypatch.setenv('https_proxy' if secure else 'http_proxy', proxy_uri)
            config = Config(
                sdk_key = 'sdk_key',
                base_uri = target_uri,
                events_uri = target_uri,
                stream_uri = target_uri,
                http = HTTPConfig(http_proxy=proxy_uri),
                diagnostic_opt_out = True)
            try:
                action(server, config, secure)
            except:
                print("test action failed (%s)" % test_desc)
                raise
            # For an insecure proxy request, our stub server behaves enough like the real thing to satisfy the
            # HTTP client, so we should be able to see the request go through. Note that the URI path will
            # actually be an absolute URI for a proxy request.
            try:
                req = server.require_request()
            except:
                print("server did not receive a request (%s)" % test_desc)
                raise
            expected_method = 'CONNECT' if secure else action_method
            assert req.method == expected_method, "method should be %s, was %s (%s)" % (expected_method, req.method, test_desc)
            if use_auth:
                expected_auth = 'Basic dXNlcjpwYXNz'
                actual_auth = req.headers.get('Proxy-Authorization')
                assert actual_auth == expected_auth, "auth header should be %s, was %s (%s)" % (expected_auth, actual_auth, test_desc)
            print("do_proxy_tests succeeded for: %s" % test_desc)
