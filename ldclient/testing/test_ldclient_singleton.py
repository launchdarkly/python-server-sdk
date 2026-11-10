import json

import ldclient
from ldclient import _reset_client, datasystem
from ldclient.config import Config
from ldclient.testing.http_util import BasicResponse, start_server
from ldclient.testing.stub_util import stream_content
from ldclient.testing.sync_util import wait_until

sdk_key = 'sdk-key'

# These are end-to-end tests like test_ldclient_end_to_end, but less detailed in terms of the client's
# network behavior because what we're really testing is the singleton mechanism.


def make_fdv2_full_transfer_events() -> str:
    intent = {'payloads': [{'id': 'p1', 'target': 1, 'intentCode': 'xfer-full', 'reason': 'payload-missing'}]}
    transferred = {'state': '(p:p1:1)', 'version': 1}
    return 'event: server-intent\ndata: %s\n\nevent: payload-transferred\ndata: %s\n\n' % (json.dumps(intent), json.dumps(transferred))


def make_streaming_config(key: str, stream_uri: str) -> Config:
    return Config(key, stream_uri=stream_uri, send_events=False, datasystem_config=datasystem.streaming().build())


def test_set_sdk_key_before_init():
    _reset_client()
    with start_server() as stream_server:
        with stream_content(make_fdv2_full_transfer_events()) as stream_handler:
            try:
                stream_server.for_path('/sdk/stream', stream_handler)

                ldclient.set_config(make_streaming_config(sdk_key, stream_server.uri))
                wait_until(ldclient.get().is_initialized, timeout=10)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()


def test_set_sdk_key_after_init():
    _reset_client()
    other_key = 'other-key'
    with start_server() as stream_server:
        with stream_content(make_fdv2_full_transfer_events()) as stream_handler:
            try:
                stream_server.for_path('/sdk/stream', BasicResponse(401))

                config = make_streaming_config(other_key, stream_server.uri)
                ldclient.set_config(config)
                assert ldclient.get().is_initialized() is False

                r = stream_server.await_request()
                assert r.headers['Authorization'] == other_key

                stream_server.for_path('/sdk/stream', stream_handler)

                # copy_with_new_sdk_key drops datasystem_config, so the test builds a new config.
                ldclient.set_config(make_streaming_config(sdk_key, stream_server.uri))
                wait_until(ldclient.get().is_initialized, timeout=30)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()


def test_set_config():
    _reset_client()
    with start_server() as stream_server:
        with stream_content(make_fdv2_full_transfer_events()) as stream_handler:
            try:
                stream_server.for_path('/sdk/stream', stream_handler)

                ldclient.set_config(Config(sdk_key, offline=True))
                assert ldclient.get().is_offline() is True

                ldclient.set_config(make_streaming_config(sdk_key, stream_server.uri))
                assert ldclient.get().is_offline() is False
                wait_until(ldclient.get().is_initialized, timeout=10)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()
