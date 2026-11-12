"""
Tests for what happens to already-started background threads when LDClient construction fails
partway through.
"""

import threading

import pytest

from ldclient.client import Config, LDClient
from ldclient.interfaces import UpdateProcessor

unreachable_uri = "http://fake"


class FailingUpdateProcessor(UpdateProcessor):
    """
    A data source that fails on start(). Reaching this is realistic: update_processor_class is a
    documented configuration hook, and the built-in data sources do real work in start().
    """

    def __init__(self, config, store, ready):
        pass

    def start(self):
        raise Exception("deliberate failure while starting the data source")

    def stop(self):
        pass

    def initialized(self):
        return False


def ldclient_threads() -> set:
    return {t.name for t in threading.enumerate() if t.name.startswith('ldclient.')}


@pytest.mark.xfail(strict=True, reason="a failure partway through __start_up leaves already-started components running with no way to reach them")
def test_failed_construction_does_not_leak_background_threads():
    """
    INVARIANT: if the constructor raises, it leaves nothing running. A caller that never receives
    a client object has no way to release anything.

    __start_up() creates the event processor - which starts a dispatcher thread, a pool of flush
    workers and two repeating timer threads - and only then starts the data system. If the data
    system fails to start, the exception propagates out of the constructor, the caller gets no
    object, and there is no handle on which to call close(). Every thread the event processor
    started stays running, along with its HTTP connection pool.

    They are daemon threads, so this does not prevent process exit, but it does leak steadily in
    any application that retries client construction, and postfork() re-runs this same path.
    """
    before = ldclient_threads()

    config = Config(
        sdk_key='SDK_KEY',
        base_uri=unreachable_uri,
        events_uri=unreachable_uri,
        stream_uri=unreachable_uri,
        update_processor_class=FailingUpdateProcessor,
        diagnostic_opt_out=True,
    )

    with pytest.raises(Exception):
        LDClient(config=config, start_wait=0)

    leaked = ldclient_threads() - before
    assert not leaked, "construction failed but left these threads running: %s" % sorted(leaked)
