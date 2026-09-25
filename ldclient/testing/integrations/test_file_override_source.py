"""
Tests for the file-based override source: its builder, initial load, multi-file merging,
absent files, change detection in both modes, failure retention, and its Info log.
"""
import logging
import os
import time
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Tuple

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.config import Config as SDKConfig
from ldclient.datasystem import custom
from ldclient.impl.integrations.files import filedata
from ldclient.impl.integrations.overrides.file_override_source import (
    _FileOverrideSource
)
from ldclient.integrations.overrides import (
    ChangeDetection,
    DuplicateKeysHandling,
    FileOverrideSourceBuilder
)
from ldclient.testing.mock_components import HangingSynchronizer
from ldclient.testing.stub_util import MockEventProcessor

TEST_TIMEOUT = 10.0
QUIET_PERIOD = 0.3

watchdog_required = pytest.mark.skipif(not filedata.have_watchdog, reason="watchdog is not installed")
yaml_required = pytest.mark.skipif(not filedata.have_yaml, reason="pyyaml is not installed")

Snapshot = Tuple[Dict[str, Any], Dict[str, Any]]


class CapturingSink:
    """Records every override snapshot it receives."""

    def __init__(self):
        self.snapshots: Queue = Queue()

    def set_overrides(self, flags, segments) -> None:
        self.snapshots.put((dict(flags), dict(segments)))

    def require_snapshot(self, timeout: float = TEST_TIMEOUT) -> Snapshot:
        try:
            return self.snapshots.get(timeout=timeout)
        except Empty:
            pytest.fail("timed out waiting for an override snapshot")

    def require_no_snapshot(self, duration: float = QUIET_PERIOD) -> None:
        try:
            snapshot = self.snapshots.get(timeout=duration)
        except Empty:
            return
        pytest.fail("received an unexpected override snapshot: %r" % (snapshot,))


def write_file(path: str, content: str) -> None:
    with open(path, 'w') as f:
        f.write(content)


def flag_values(snapshot: Snapshot) -> Dict[str, Any]:
    """The single variation of each flag in a snapshot, keyed by flag key."""
    return {key: flag.variations[0] for key, flag in snapshot[0].items()}


@pytest.fixture
def sources():
    started: List[_FileOverrideSource] = []
    yield started
    for source in started:
        source.close()


def build_source(sources, paths, configure=None) -> Tuple[_FileOverrideSource, CapturingSink]:
    builder = FileOverrideSourceBuilder(paths)
    if configure is not None:
        configure(builder)
    source = builder.build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    sink = CapturingSink()
    source.start(sink)
    sources.append(source)
    return source, sink


def info_lines(caplog) -> List[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]


def require_info_line(caplog, expected: str) -> None:
    """Waits for an Info line equal to the expected text. The source logs after it hands the snapshot to the sink."""
    deadline = time.time() + TEST_TIMEOUT
    while True:
        if expected in info_lines(caplog):
            return
        if time.time() > deadline:
            pytest.fail("timed out waiting for the Info line %r; Info output: %r" % (expected, info_lines(caplog)))
        time.sleep(0.01)


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def test_builder_requires_paths():
    with pytest.raises(ValueError):
        FileOverrideSourceBuilder([]).build(SDKConfig('SDK_KEY'))


def test_builder_accepts_a_single_path_string(tmp_path):
    path = os.path.join(str(tmp_path), 'overrides.json')
    source = FileOverrideSourceBuilder(path).build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._paths == [path]


def test_builder_resolves_relative_paths():
    source = FileOverrideSourceBuilder(['relative/overrides.json']).build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._paths == [os.path.abspath('relative/overrides.json')]


def test_builder_polls_by_default(tmp_path):
    path = os.path.join(str(tmp_path), 'overrides.json')
    source = FileOverrideSourceBuilder([path]).build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._change_detection == ChangeDetection.POLLING
    assert source._poll_interval == FileOverrideSourceBuilder.DEFAULT_POLL_INTERVAL
    assert source._duplicate_keys_handling == DuplicateKeysHandling.FAIL


def test_builder_rejects_unknown_change_detection():
    with pytest.raises(ValueError) as excinfo:
        FileOverrideSourceBuilder(['a']).change_detection('notify')
    assert 'notify' in str(excinfo.value)


def test_builder_rejects_unknown_duplicate_keys_handling():
    with pytest.raises(ValueError):
        FileOverrideSourceBuilder(['a']).duplicate_keys_handling('merge')


def test_builder_accepts_string_values():
    builder = FileOverrideSourceBuilder(['a']).change_detection('polling').duplicate_keys_handling('ignore')
    source = builder.build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._change_detection == ChangeDetection.POLLING
    assert source._duplicate_keys_handling == DuplicateKeysHandling.IGNORE


def test_builder_clamps_poll_interval_to_the_minimum(caplog):
    with caplog.at_level(logging.WARNING):
        source = FileOverrideSourceBuilder(['a']).poll_interval(0.001).build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._poll_interval == FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL
    assert any('below the minimum' in r.getMessage() for r in caplog.records)

    source = FileOverrideSourceBuilder(['a']).poll_interval(2.5).build(SDKConfig('SDK_KEY'))
    assert isinstance(source, _FileOverrideSource)
    assert source._poll_interval == 2.5


def test_builder_rejects_watching_without_watchdog(monkeypatch):
    import ldclient.integrations.overrides as module
    monkeypatch.setattr(module, 'have_watchdog', False)
    with pytest.raises(ValueError) as excinfo:
        FileOverrideSourceBuilder(['a']).change_detection(ChangeDetection.WATCHING).build(SDKConfig('SDK_KEY'))
    assert 'watchdog' in str(excinfo.value)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def test_source_loads_initial_data_synchronously(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true, "flag2": "x"}, "segments": {"seg": {"key": "seg"}}}')
    source, sink = build_source(sources, [path])
    # The snapshot was delivered before start returned.
    snapshot = sink.snapshots.get_nowait()
    assert flag_values(snapshot) == {'flag1': True, 'flag2': 'x'}
    assert list(snapshot[1].keys()) == ['seg']
    assert snapshot[0]['flag1'].is_override is False, "the source supplies unmarked definitions; the SDK marks them"


@yaml_required
def test_source_loads_yaml(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.yaml')
    write_file(path, 'flagValues:\n  yaml-flag: "override-value"\n')
    _, sink = build_source(sources, [path])
    assert flag_values(sink.require_snapshot()) == {'yaml-flag': 'override-value'}


def test_source_merges_files_in_configured_order(tmp_path, sources):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"shared": "first", "only-first": 1}}')
    write_file(second, '{"flagValues": {"shared": "second", "only-second": 2}}')
    _, sink = build_source(sources, [first, second], lambda b: b.duplicate_keys_handling(DuplicateKeysHandling.IGNORE))
    assert flag_values(sink.require_snapshot()) == {'shared': 'first', 'only-first': 1, 'only-second': 2}


def test_source_duplicate_keys_fail_by_default(tmp_path, sources, caplog):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"shared": "first"}}')
    write_file(second, '{"flagValues": {"shared": "second"}}')
    with caplog.at_level(logging.ERROR):
        _, sink = build_source(sources, [first, second])
    # The load failed, so no snapshot was supplied and the failure was logged.
    sink.require_no_snapshot()
    assert any("is specified by multiple files" in r.getMessage() for r in caplog.records)


def test_source_starts_with_missing_file(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'not-yet.json')
    _, sink = build_source(sources, [path], lambda b: b.poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
    # A missing file contributes no overrides. The initial snapshot is empty.
    assert sink.require_snapshot() == ({}, {})
    # Once the file appears, the change detection picks it up.
    write_file(path, '{"flagValues": {"flag1": true}}')
    assert flag_values(sink.require_snapshot()) == {'flag1': True}


def test_source_missing_file_contributes_no_entries(tmp_path, sources):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"from-first": true}}')

    # Step 1: one configured file exists and one does not. The existing file applies.
    _, sink = build_source(sources, [first, second], lambda b: b.poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
    assert flag_values(sink.require_snapshot()) == {'from-first': True}

    # Step 2: the second file appears. Both apply.
    write_file(second, '{"flagValues": {"from-second": true}}')
    assert flag_values(sink.require_snapshot()) == {'from-first': True, 'from-second': True}

    # Step 3: the second file is deleted. Its overrides are removed.
    os.remove(second)
    assert flag_values(sink.require_snapshot()) == {'from-first': True}

    # Step 4: the last file is deleted. The layer is cleared.
    os.remove(first)
    assert sink.require_snapshot() == ({}, {})


def test_source_logs_overrides_in_effect_on_each_change(tmp_path, sources, caplog):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"flag1": true, "flag2": false}, "segments": {"seg": {"key": "seg"}}}')

    with caplog.at_level(logging.INFO):
        # Step 1: at startup, one file supplies entries and the other is absent.
        _, sink = build_source(sources, [first, second], lambda b: b.poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
        sink.require_snapshot()
        require_info_line(caplog, "Flag overrides in effect: 2 flags, 1 segment (%s: 2 flags, 1 segment; %s: absent)" % (first, second))

        # Step 2: the absent file appears with one entry.
        write_file(second, '{"flagValues": {"flag3": true}}')
        sink.require_snapshot()
        require_info_line(caplog, "Flag overrides in effect: 3 flags, 1 segment (%s: 2 flags, 1 segment; %s: 1 flag)" % (first, second))

        # Step 3: both files are deleted. Nothing is in effect.
        os.remove(first)
        os.remove(second)
        sink.require_snapshot()
        require_info_line(caplog, "Flag overrides: none in effect (%s: absent; %s: absent)" % (first, second))


def test_source_logs_none_in_effect_at_startup_without_files(tmp_path, sources, caplog):
    path = os.path.join(str(tmp_path), 'overrides.json')
    with caplog.at_level(logging.INFO):
        _, sink = build_source(sources, [path])
        sink.require_snapshot()
        require_info_line(caplog, "Flag overrides: none in effect (%s: absent)" % path)


def test_source_logs_a_present_file_with_no_entries(tmp_path, sources, caplog):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{}')
    with caplog.at_level(logging.INFO):
        _, sink = build_source(sources, [path])
        sink.require_snapshot()
        require_info_line(caplog, "Flag overrides: none in effect (%s: no entries)" % path)


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

@watchdog_required
def test_watching_mode_is_quiet_when_file_is_absent(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'not-yet.json')
    _, sink = build_source(sources, [path], lambda b: b.change_detection(ChangeDetection.WATCHING))
    assert sink.require_snapshot() == ({}, {})
    sink.require_no_snapshot()
    write_file(path, '{"flagValues": {"flag1": true}}')
    assert flag_values(sink.require_snapshot()) == {'flag1': True}


@watchdog_required
def test_watching_mode_reloads_on_change(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    source, sink = build_source(sources, [path], lambda b: b.change_detection(ChangeDetection.WATCHING))
    assert isinstance(source._change_detector, filedata.Watcher)
    assert flag_values(sink.require_snapshot()) == {'flag1': True}
    write_file(path, '{"flagValues": {"flag1": false}}')
    assert flag_values(sink.require_snapshot()) == {'flag1': False}


def test_polling_mode_reloads_on_change(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    source, sink = build_source(sources, [path], lambda b: b.change_detection(ChangeDetection.POLLING).poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
    assert isinstance(source._change_detector, filedata.Poller)
    assert flag_values(sink.require_snapshot()) == {'flag1': True}
    write_file(path, '{"flagValues": {"flag1": false}}')
    assert flag_values(sink.require_snapshot()) == {'flag1': False}


@pytest.mark.parametrize("mode", [ChangeDetection.POLLING, pytest.param(ChangeDetection.WATCHING, marks=watchdog_required)])
def test_source_retains_last_good_data_across_malformed_edit(tmp_path, sources, caplog, mode):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    with caplog.at_level(logging.ERROR):
        _, sink = build_source(sources, [path], lambda b: b.change_detection(mode).poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
        sink.require_snapshot()

        # A malformed edit produces no snapshot: the previously applied overrides stay in
        # effect because the sink is never called. The failure is logged.
        write_file(path, '{"flagValues"')
        sink.require_no_snapshot(1.5)
        deadline = time.time() + TEST_TIMEOUT
        while not any('Unable to load flag data' in r.getMessage() for r in caplog.records):
            assert time.time() < deadline, "the load failure was not logged"
            time.sleep(0.01)

        # Fixing the file recovers, through the change notification or the failure retry.
        write_file(path, '{"flagValues": {"flag1": false}}')
        assert flag_values(sink.require_snapshot()) == {'flag1': False}


def test_source_retries_a_failed_load_without_a_change_signal(tmp_path, sources, caplog):
    # The fixed content has the same size as the malformed content and the file keeps its
    # modification time, so the poller sees no change. Only the automatic retry can recover.
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    with caplog.at_level(logging.ERROR):
        _, sink = build_source(sources, [path], lambda b: b.poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
        sink.require_snapshot()

        malformed = '{"flagValues": {"flag1": false}'
        fixed = '{"flagValues": {"flag1":false}}'
        assert len(malformed) == len(fixed)
        write_file(path, malformed)
        deadline = time.time() + TEST_TIMEOUT
        while not any('Unable to load flag data' in r.getMessage() for r in caplog.records):
            assert time.time() < deadline, "the load failure was not logged"
            time.sleep(0.01)
        observed = os.stat(path)
        write_file(path, fixed)
        os.utime(path, ns=(observed.st_atime_ns, observed.st_mtime_ns))
        assert os.stat(path).st_size == observed.st_size
        assert flag_values(sink.require_snapshot()) == {'flag1': False}


def test_source_does_not_supply_snapshots_after_close(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    source, sink = build_source(sources, [path], lambda b: b.poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL))
    sink.require_snapshot()
    source.close()
    write_file(path, '{"flagValues": {"flag1": false}}')
    sink.require_no_snapshot(2.5)


def test_source_close_is_idempotent(tmp_path, sources):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{}')
    source, _ = build_source(sources, [path])
    source.close()
    source.close()


def test_source_start_after_close_does_nothing(tmp_path):
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    source = FileOverrideSourceBuilder([path]).build(SDKConfig('SDK_KEY'))
    source.close()
    sink = CapturingSink()
    source.start(sink)
    sink.require_no_snapshot(0.2)


# ---------------------------------------------------------------------------
# Through the client
# ---------------------------------------------------------------------------

def test_file_overrides_end_to_end(tmp_path):
    # An operator writes, edits, and empties an override file. Evaluations follow without any
    # client restart, even though the client never obtains data from LaunchDarkly.
    path = os.path.join(str(tmp_path), 'overrides.json')
    write_file(path, '{}')
    user = Context.create('user-key')
    source = FileOverrideSourceBuilder([path]).poll_interval(FileOverrideSourceBuilder.MINIMUM_POLL_INTERVAL)
    datasystem = custom().synchronizers(HangingSynchronizer().builder).overrides(source).build()
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem, event_processor_class=MockEventProcessor)

    def eventually(predicate, message: str) -> None:
        deadline = time.time() + TEST_TIMEOUT
        while not predicate():
            assert time.time() < deadline, message
            time.sleep(0.05)

    with LDClient(config, start_wait=0) as client:
        # Not initialized and no override present: the default is served.
        detail = client.variation_detail('overridden-flag', user, False)
        assert detail.value is False
        assert detail.reason == {'kind': 'ERROR', 'errorKind': 'CLIENT_NOT_READY'}

        # An operator adds an override. The running client picks it up.
        write_file(path, '{"flagValues": {"overridden-flag": true}}')
        eventually(lambda: client.variation('overridden-flag', user, False) is True, "the override was not picked up")

        # The override changes value.
        write_file(path, '{"flagValues": {"overridden-flag": false}}')

        def changed() -> bool:
            detail = client.variation_detail('overridden-flag', user, True)
            return detail.value is False and detail.reason.get('overrideAffected') is True

        eventually(changed, "the changed override was not picked up")

        # The override is removed. The not-initialized short-circuit returns.
        write_file(path, '{}')
        eventually(lambda: client.variation_detail('overridden-flag', user, False).reason.get('errorKind') == 'CLIENT_NOT_READY', "the removed override was not picked up")
