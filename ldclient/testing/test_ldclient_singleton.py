import json
import threading
import time

import pytest

import ldclient
from ldclient import _reset_client
from ldclient.client import LDClient
from ldclient.config import Config
from ldclient.testing.http_util import BasicResponse, start_server
from ldclient.testing.stub_util import make_put_event, stream_content
from ldclient.testing.sync_util import wait_until

sdk_key = 'sdk-key'

# These are end-to-end tests like test_ldclient_end_to_end, but less detailed in terms of the client's
# network behavior because what we're really testing is the singleton mechanism.


def test_set_sdk_key_before_init():
    _reset_client()
    with start_server() as stream_server:
        with stream_content(make_put_event()) as stream_handler:
            try:
                stream_server.for_path('/all', stream_handler)

                ldclient.set_config(Config(sdk_key, stream_uri=stream_server.uri, send_events=False))
                wait_until(ldclient.get().is_initialized, timeout=10)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()


def test_set_sdk_key_after_init():
    _reset_client()
    other_key = 'other-key'
    with start_server() as stream_server:
        with stream_content(make_put_event()) as stream_handler:
            try:
                stream_server.for_path('/all', BasicResponse(401))

                config = Config(other_key, stream_uri=stream_server.uri, send_events=False)
                ldclient.set_config(config)
                assert ldclient.get().is_initialized() is False

                r = stream_server.await_request()
                assert r.headers['Authorization'] == other_key

                stream_server.for_path('/all', stream_handler)

                ldclient.set_config(config.copy_with_new_sdk_key(sdk_key))
                wait_until(ldclient.get().is_initialized, timeout=30)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()


def test_set_config():
    _reset_client()
    with start_server() as stream_server:
        with stream_content(make_put_event()) as stream_handler:
            try:
                stream_server.for_path('/all', stream_handler)

                ldclient.set_config(Config(sdk_key, offline=True))
                assert ldclient.get().is_offline() is True

                ldclient.set_config(Config(sdk_key, stream_uri=stream_server.uri, send_events=False))
                assert ldclient.get().is_offline() is False
                wait_until(ldclient.get().is_initialized, timeout=10)

                r = stream_server.await_request()
                assert r.headers['Authorization'] == sdk_key
            finally:
                _reset_client()


@pytest.mark.xfail(strict=True, reason="set_config() holds the global write lock across the old client's close()")
def test_set_config_does_not_block_concurrent_get():
    """
    INVARIANT: reconfiguring the shared client does not stall unrelated threads that are only
    reading it. ldclient.get() is on the hot path of every flag evaluation in an application.

    set_config() holds the global write lock (ldclient/__init__.py) across both the construction
    of the replacement client and old_client.close(). ReadWriteLock.lock() holds the underlying
    mutex for its whole duration, so every concurrent ldclient.get() - which takes a read lock -
    blocks until both of those finish.

    That couples a network-dependent shutdown to a lock every evaluating thread needs. A close()
    that stalls stalls the entire application, not just the thread that called set_config().
    """
    _reset_client()
    close_duration = 3.0
    real_close = LDClient.close

    def slow_close(self):
        time.sleep(close_duration)
        real_close(self)

    try:
        ldclient.set_config(Config(sdk_key, offline=True))
        ldclient.get()

        LDClient.close = slow_close  # type: ignore[method-assign]

        reconfiguring = threading.Event()

        def reconfigure():
            reconfiguring.set()
            ldclient.set_config(Config(sdk_key, offline=True))

        threading.Thread(target=reconfigure, name='reconfigure', daemon=True).start()
        assert reconfiguring.wait(5)
        time.sleep(0.2)  # let set_config get inside the write lock

        started = time.time()
        ldclient.get()
        blocked_for = time.time() - started

        assert blocked_for < close_duration / 2, (
            "ldclient.get() blocked for %.1fs while set_config() was closing the previous client" % blocked_for
        )
    finally:
        LDClient.close = real_close  # type: ignore[method-assign]
        _reset_client()
