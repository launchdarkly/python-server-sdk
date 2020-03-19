from ldclient.client import LDClient, Config
from testing.http_util import start_secure_server
import pytest
import sys

# These tests are skipped in Python 3.3 because the embedded HTTPS server does not work correctly, causing a
# TLS handshake failure on the client side. It's unclear whether this is a problem with the self-signed
# certificate we are using or with some other server settings, but it does not appear to be a client-side
# problem.

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_cannot_connect_with_selfsigned_cert_if_ssl_verify_is_true():
    with start_secure_server() as server:
        server.setup_json_response('/sdk/latest-all', { 'flags': {}, 'segments': {} })
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False
        )
        with LDClient(config = config, start_wait = 1.5) as client:
            assert not client.is_initialized()

@pytest.mark.skipif(sys.version_info.major == 3 and sys.version_info.minor == 3, reason = "test is skipped in Python 3.3")
def test_can_connect_with_selfsigned_cert_if_ssl_verify_is_false():
    with start_secure_server() as server:
        server.setup_json_response('/sdk/latest-all', { 'flags': {}, 'segments': {} })
        config = Config(
            sdk_key = 'sdk_key',
            base_uri = server.uri,
            stream = False,
            send_events = False,
            verify_ssl = False
        )
        with LDClient(config = config) as client:
            assert client.is_initialized()
