"""
Tests for the fork warning in LDClient.

Threads do not survive a fork. A child process that inherits a client has no
background threads, so the client must log a warning once per process until
``postfork()`` is called.
"""
import logging
import os
import threading
import warnings
from typing import Any, Dict

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.migrations import Stage
from ldclient.testing.stub_util import MockEventProcessor, MockUpdateProcessor

unreachable_uri = "http://fake"

context = Context.builder('xyz').build()

WARNING_TEXT = "forked after the LDClient was created"


def make_config(**kwargs) -> Config:
    params: Dict[str, Any] = dict(
        sdk_key='SDK_KEY',
        base_uri=unreachable_uri,
        events_uri=unreachable_uri,
        stream_uri=unreachable_uri,
        event_processor_class=MockEventProcessor,
        update_processor_class=MockUpdateProcessor,
    )
    params.update(kwargs)
    return Config(**params)


def make_client(**kwargs) -> LDClient:
    return LDClient(config=make_config(**kwargs), start_wait=0)


def fork_warnings(caplog) -> list:
    return [r for r in caplog.records if r.levelno == logging.WARNING and WARNING_TEXT in r.getMessage()]


def pretend_forked(monkeypatch, offset: int = 1):
    """Make os.getpid report a pid other than the one the client recorded.

    The client is created before this patch is applied, so the client sees the
    real pid as the owner and the patched pid as a child.
    """
    fake_pid = os.getpid() + offset
    monkeypatch.setattr(os, "getpid", lambda: fake_pid)


def test_no_warning_when_pid_has_not_changed(caplog):
    with make_client() as client:
        client.variation("flag", context, False)
        client.variation_detail("flag", context, False)
        client.migration_variation("flag", context, Stage.OFF)
        client.identify(context)
        client.track("event", context)
        client.all_flags_state(context)
        client.flush()
    assert fork_warnings(caplog) == []


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda c: c.variation("flag", context, False), id="variation"),
        pytest.param(lambda c: c.variation_detail("flag", context, False), id="variation_detail"),
        pytest.param(lambda c: c.migration_variation("flag", context, Stage.OFF), id="migration_variation"),
        pytest.param(lambda c: c.all_flags_state(context), id="all_flags_state"),
        pytest.param(lambda c: c.identify(context), id="identify"),
        pytest.param(lambda c: c.track("event", context), id="track"),
        pytest.param(lambda c: c.flush(), id="flush"),
    ],
)
def test_warns_on_first_call_after_pid_change(caplog, monkeypatch, call):
    with make_client() as client:
        pretend_forked(monkeypatch)
        call(client)
        assert len(fork_warnings(caplog)) == 1


def test_warns_on_track_migration_op(caplog, monkeypatch):
    with make_client() as client:
        _, tracker = client.migration_variation("flag", context, Stage.OFF)
        pretend_forked(monkeypatch)
        client.track_migration_op(tracker)
        assert len(fork_warnings(caplog)) == 1


def test_warns_on_close(caplog, monkeypatch):
    client = make_client()
    pretend_forked(monkeypatch)
    client.close()
    assert len(fork_warnings(caplog)) == 1


def test_warning_names_postfork_and_says_what_stops_working(caplog, monkeypatch):
    with make_client() as client:
        pretend_forked(monkeypatch)
        client.variation("flag", context, False)
    message = fork_warnings(caplog)[0].getMessage()
    assert "LDClient.postfork()" in message
    assert "flag updates and analytics events will not work" in message


def test_warns_only_once_per_process_across_many_calls(caplog, monkeypatch):
    with make_client() as client:
        pretend_forked(monkeypatch)
        for _ in range(5):
            client.variation("flag", context, False)
        client.variation_detail("flag", context, False)
        client.migration_variation("flag", context, Stage.OFF)
        client.identify(context)
        client.track("event", context)
        client.all_flags_state(context)
        client.flush()
    assert len(fork_warnings(caplog)) == 1


def test_warns_only_once_when_many_threads_evaluate_at_the_same_time(caplog, monkeypatch):
    with make_client() as client:
        pretend_forked(monkeypatch)
        start = threading.Barrier(16)

        def worker():
            start.wait()
            for _ in range(50):
                client.variation("flag", context, False)

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert len(fork_warnings(caplog)) == 1


def test_postfork_clears_the_warning(caplog, monkeypatch):
    with make_client() as client:
        pretend_forked(monkeypatch)
        client.variation("flag", context, False)
        assert len(fork_warnings(caplog)) == 1

        client.postfork(0)
        client.variation("flag", context, False)
        client.flush()
    assert len(fork_warnings(caplog)) == 1


def test_warns_again_in_a_grandchild_process(caplog, monkeypatch):
    with make_client() as client:
        pretend_forked(monkeypatch, offset=1)
        client.variation("flag", context, False)
        assert len(fork_warnings(caplog)) == 1

        # The "already warned" state is copied into a grandchild on fork. It must still warn there.
        pretend_forked(monkeypatch, offset=2)
        client.variation("flag", context, False)
        assert len(fork_warnings(caplog)) == 2


def test_no_warning_in_offline_mode(caplog, monkeypatch):
    with make_client(offline=True) as client:
        pretend_forked(monkeypatch)
        client.variation("flag", context, False)
        client.variation_detail("flag", context, False)
        client.identify(context)
        client.track("event", context)
        client.all_flags_state(context)
        client.flush()
    assert fork_warnings(caplog) == []


def test_no_warning_in_ldd_mode_with_events_disabled(caplog, monkeypatch):
    with make_client(use_ldd=True, send_events=False) as client:
        pretend_forked(monkeypatch)
        client.variation("flag", context, False)
        client.identify(context)
        client.flush()
    assert fork_warnings(caplog) == []


def test_warns_in_ldd_mode_with_events_enabled(caplog, monkeypatch):
    with make_client(use_ldd=True, send_events=True) as client:
        pretend_forked(monkeypatch)
        client.variation("flag", context, False)
    assert len(fork_warnings(caplog)) == 1


class _CountingHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.count = 0

    def emit(self, record):
        if WARNING_TEXT in record.getMessage():
            self.count += 1


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is not supported on this platform")
def test_real_fork_warns_in_child_and_stops_after_postfork(caplog):
    client = make_client()
    try:
        read_fd, write_fd = os.pipe()
        with warnings.catch_warnings():
            # Python 3.12+ warns about fork() in a multi-threaded process. That is the
            # situation this feature exists for, so the warning is expected here.
            warnings.simplefilter("ignore", DeprecationWarning)
            pid = os.fork()

        if pid == 0:
            # Child. Never return into pytest; always leave through os._exit.
            try:
                os.close(read_fd)
                handler = _CountingHandler()
                logging.getLogger('ldclient.util').addHandler(handler)

                client.variation("flag", context, False)
                client.variation("flag", context, False)
                before_postfork = handler.count

                client.postfork(0)
                client.variation("flag", context, False)
                after_postfork = handler.count

                os.write(write_fd, f"{before_postfork},{after_postfork}".encode())
                os.close(write_fd)
                os._exit(0)
            except BaseException:
                os._exit(1)

        os.close(write_fd)
        chunks = []
        while True:
            chunk = os.read(read_fd, 1024)
            if not chunk:
                break
            chunks.append(chunk)
        os.close(read_fd)
        _, status = os.waitpid(pid, 0)

        assert os.waitstatus_to_exitcode(status) == 0
        assert b"".join(chunks).decode() == "1,1"

        # The parent process was not forked, so it must not warn.
        client.variation("flag", context, False)
        assert fork_warnings(caplog) == []
    finally:
        client.close()
