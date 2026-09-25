import os
import threading
import time
from queue import Empty, Queue
from typing import Any, Dict, List

import pytest

from ldclient.impl.integrations.files.filedata import (
    Document,
    DuplicateKeyError,
    DuplicateKeysHandling,
    FileReadError,
    FileSummary,
    MergeResult,
    Poller,
    Reloader,
    Watcher,
    abs_file_paths,
    have_watchdog,
    have_yaml,
    load_files,
    make_flag_with_value,
    merge,
    parse_document,
    read_file
)
from ldclient.impl.model import FeatureFlag, Segment

TEST_TIMEOUT = 5.0

# Timings for the reloader and poller tests. They are generous multiples of the configured
# delays so the tests stay deterministic on a loaded machine.
SHORT_DELAY = 0.05
QUIET_PERIOD = 0.3


def write_file(path: str, content: str) -> None:
    with open(path, 'w') as f:
        f.write(content)


def take(queue: Queue, timeout: float = TEST_TIMEOUT) -> Any:
    try:
        return queue.get(timeout=timeout)
    except Empty:
        pytest.fail("timed out waiting for a callback")


def require_quiet(queue: Queue, duration: float = QUIET_PERIOD) -> None:
    try:
        item = queue.get(timeout=duration)
    except Empty:
        return
    pytest.fail("received an unexpected callback: %r" % (item,))


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def test_parse_json_document():
    document = parse_document(b'{"flags": {"flag1": {"key": "flag1", "version": 3, "on": true}}, "flagValues": {"flag2": "value2"}, "segments": {"seg1": {"key": "seg1", "version": 2, "included": ["user1"]}}}')
    assert list(document.flags.keys()) == ['flag1']
    assert isinstance(document.flags['flag1'], FeatureFlag)
    assert document.flags['flag1'].version == 3
    assert document.flags['flag1'].on is True
    assert document.flag_values == {'flag2': 'value2'}
    assert isinstance(document.segments['seg1'], Segment)
    assert document.segments['seg1'].included == {'user1'}


def test_parse_json_document_with_leading_whitespace_uses_json_parser():
    # Tabs inside strings are invalid JSON but valid YAML, so only the JSON parser rejects this.
    with pytest.raises(ValueError):
        parse_document(b'  \n {"flagValues": {"flag1": "a\tb"}}')


def test_parse_yaml_document():
    if not have_yaml:
        pytest.skip("pyyaml is not installed")
    document = parse_document(b'---\nflags:\n  flag1:\n    key: flag1\n    "on": true\nflagValues:\n  flag2: value2\nsegments:\n  seg1:\n    key: seg1\n')
    assert document.flags['flag1'].on is True
    assert document.flag_values == {'flag2': 'value2'}
    assert list(document.segments.keys()) == ['seg1']


def test_parse_empty_document_has_no_entries():
    document = parse_document(b'')
    assert document == Document()
    document = parse_document(b'{}')
    assert document == Document()


def test_parse_fills_in_missing_key_and_version():
    document = parse_document(b'{"flags": {"flag1": {"on": true}}, "segments": {"seg1": {}}}')
    assert document.flags['flag1'].key == 'flag1'
    assert document.flags['flag1'].version == 1
    assert document.segments['seg1'].key == 'seg1'
    assert document.segments['seg1'].version == 1


def test_parse_rejects_documents_that_are_not_objects():
    if have_yaml:
        with pytest.raises(ValueError):
            parse_document(b'- a\n- b\n')
    with pytest.raises(ValueError):
        parse_document(b'{"flags": ["not", "an", "object"]}')
    with pytest.raises(ValueError):
        parse_document(b'{"flags": {"flag1": "not an object"}}')
    with pytest.raises(ValueError):
        parse_document(b'{"segments": {"seg1": 3}}')
    with pytest.raises(ValueError):
        parse_document(b'{"flagValues": 3}')


def test_parse_rejects_malformed_json():
    with pytest.raises(ValueError):
        parse_document(b'{"flagValues"')


def test_parse_validates_definition_property_types():
    with pytest.raises(ValueError):
        parse_document(b'{"flags": {"flag1": {"key": "flag1", "version": "not a number"}}}')
    with pytest.raises(ValueError):
        parse_document(b'{"segments": {"seg1": {"key": "seg1", "version": 1, "included": "not a list"}}}')


def test_read_file_json_and_errors(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    assert read_file(path).flag_values == {'flag1': True}

    with pytest.raises(FileReadError) as excinfo:
        read_file(os.path.join(str(tmp_path), 'missing.json'))
    assert excinfo.value.path == os.path.join(str(tmp_path), 'missing.json')
    assert 'unable to read file' in str(excinfo.value)

    write_file(path, '{"flagValues"')
    with pytest.raises(FileReadError) as excinfo:
        read_file(path)
    assert 'error parsing file' in str(excinfo.value)
    assert path in str(excinfo.value)


def test_abs_file_paths():
    paths = abs_file_paths(['relative/data.json', '/absolute/data.json'])
    assert paths[0] == os.path.abspath('relative/data.json')
    assert paths[1] == '/absolute/data.json'


def test_make_flag_with_value_is_off_and_serves_the_value():
    flag = make_flag_with_value('flag1', 'value1')
    assert flag.key == 'flag1'
    assert flag.version == 1
    assert flag.on is False
    assert flag.off_variation == 0
    assert flag.variations == ['value1']
    assert flag.to_json_dict() == {'key': 'flag1', 'version': 1, 'on': False, 'offVariation': 0, 'variations': ['value1']}


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

def doc(json_text: str) -> Document:
    return parse_document(json_text.encode('utf-8'))


def test_merge_combines_documents():
    result = merge([
        doc('{"flags": {"flag1": {"key": "flag1", "version": 1}}, "segments": {"seg1": {"key": "seg1", "version": 1}}}'),
        doc('{"flagValues": {"flag2": "value2"}}'),
    ], DuplicateKeysHandling.FAIL)
    assert list(result.flags.keys()) == ['flag1', 'flag2']
    assert list(result.segments.keys()) == ['seg1']
    assert result.flags['flag2'].variations == ['value2']
    assert result.flags['flag2'].on is False


def test_merge_duplicate_keys_fail():
    documents = [doc('{"flagValues": {"flag1": "a"}}'), doc('{"flags": {"flag1": {"key": "flag1", "version": 1}}}')]
    with pytest.raises(DuplicateKeyError) as excinfo:
        merge(documents, DuplicateKeysHandling.FAIL)
    assert "flag 'flag1' is specified by multiple files" in str(excinfo.value)

    segment_documents = [doc('{"segments": {"seg1": {"key": "seg1"}}}'), doc('{"segments": {"seg1": {"key": "seg1"}}}')]
    with pytest.raises(DuplicateKeyError) as excinfo:
        merge(segment_documents, DuplicateKeysHandling.FAIL)
    assert "segment 'seg1' is specified by multiple files" in str(excinfo.value)


def test_merge_duplicate_keys_within_one_document_between_flags_and_flag_values_fail():
    documents = [doc('{"flags": {"flag1": {"key": "flag1", "version": 1}}, "flagValues": {"flag1": "a"}}')]
    with pytest.raises(DuplicateKeyError):
        merge(documents, DuplicateKeysHandling.FAIL)


def test_merge_duplicate_keys_ignore_keeps_first():
    result = merge([
        doc('{"flagValues": {"flag1": "first"}}'),
        doc('{"flagValues": {"flag1": "second", "flag2": "other"}}'),
    ], DuplicateKeysHandling.IGNORE)
    assert result.flags['flag1'].variations == ['first']
    assert list(result.flags.keys()) == ['flag1', 'flag2']


def test_merge_preserves_document_order():
    result = merge([
        doc('{"flagValues": {"b": 1, "a": 2}}'),
        doc('{"flagValues": {"c": 3}}'),
        doc('{"flags": {"d": {"key": "d", "version": 1}}}'),
    ], DuplicateKeysHandling.FAIL)
    assert list(result.flags.keys()) == ['b', 'a', 'c', 'd']


def test_merge_counts_entries_kept_from_each_document():
    result = merge([
        doc('{"flagValues": {"flag1": true, "flag2": false}, "segments": {"seg": {"key": "seg"}}}'),
        doc('{"flagValues": {"flag2": true, "flag3": true}}'),
        doc('{}'),
    ], DuplicateKeysHandling.IGNORE)
    assert [(d.flags, d.segments) for d in result.documents] == [(2, 1), (1, 0), (0, 0)]


# ---------------------------------------------------------------------------
# Loading files
# ---------------------------------------------------------------------------

def test_load_files_fails_on_missing_path_by_default(tmp_path):
    present = os.path.join(str(tmp_path), 'present.json')
    missing = os.path.join(str(tmp_path), 'missing.json')
    write_file(present, '{"flagValues": {"flag1": true}}')
    with pytest.raises(FileReadError) as excinfo:
        load_files([present, missing], DuplicateKeysHandling.FAIL)
    assert excinfo.value.path == missing


def test_load_files_skips_missing_paths_when_configured(tmp_path):
    present = os.path.join(str(tmp_path), 'present.json')
    missing = os.path.join(str(tmp_path), 'missing.json')
    write_file(present, '{"flagValues": {"flag1": true}}')
    result = load_files([present, missing], DuplicateKeysHandling.FAIL, skip_missing_paths=True)
    assert list(result.flags.keys()) == ['flag1']
    assert result.files == [FileSummary(path=present, present=True, flags=1), FileSummary(path=missing, present=False)]


def test_load_files_reports_parse_error_with_path(tmp_path):
    path = os.path.join(str(tmp_path), 'bad.json')
    write_file(path, '{"flagValues"')
    with pytest.raises(FileReadError) as excinfo:
        load_files([path], DuplicateKeysHandling.FAIL)
    assert excinfo.value.path == path


def test_load_files_reports_duplicate_keys(tmp_path):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"flag1": "first"}}')
    write_file(second, '{"flagValues": {"flag1": "second"}}')
    with pytest.raises(DuplicateKeyError):
        load_files([first, second], DuplicateKeysHandling.FAIL)
    result = load_files([first, second], DuplicateKeysHandling.IGNORE)
    assert result.flags['flag1'].variations == ['first']
    assert result.files[0].flags == 1
    assert result.files[1].flags == 0


# ---------------------------------------------------------------------------
# Reloader
# ---------------------------------------------------------------------------

class ReloaderFixture:
    def __init__(self, tmp_path, initial_content: str, **kwargs):
        self.path = os.path.join(str(tmp_path), 'data.json')
        self.applied: Queue = Queue()
        self.errored: Queue = Queue()
        write_file(self.path, initial_content)
        options: Dict[str, Any] = dict(
            paths=[self.path],
            duplicate_keys_handling=DuplicateKeysHandling.FAIL,
            apply=self.applied.put,
            on_error=self.errored.put,
        )
        options.update(kwargs)
        self.reloader = Reloader(**options)

    def write(self, content: str) -> None:
        write_file(self.path, content)

    def require_applied(self) -> MergeResult:
        return take(self.applied)

    def require_errored(self) -> Exception:
        return take(self.errored)

    def require_quiet(self, duration: float = QUIET_PERIOD) -> None:
        deadline = time.time() + duration
        while time.time() < deadline:
            if not self.applied.empty():
                pytest.fail("unexpected apply call")
            if not self.errored.empty():
                pytest.fail("unexpected on_error call")
            time.sleep(0.01)

    def close(self) -> None:
        self.reloader.close()


@pytest.fixture
def make_fixture(tmp_path):
    fixtures: List[ReloaderFixture] = []

    def factory(initial_content: str, **kwargs) -> ReloaderFixture:
        fixture = ReloaderFixture(tmp_path, initial_content, **kwargs)
        fixtures.append(fixture)
        return fixture

    yield factory
    for fixture in fixtures:
        fixture.close()


def test_reloader_initial_load(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    result = f.require_applied()
    assert list(result.flags.keys()) == ['flag1']
    assert result.files == [FileSummary(path=f.path, present=True, flags=1)]


def test_reloader_fails_on_missing_path_by_default(make_fixture, tmp_path):
    missing = os.path.join(str(tmp_path), 'missing.json')
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader = Reloader([f.path, missing], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    err = f.require_errored()
    assert isinstance(err, FileReadError)
    assert err.path == missing
    f.require_quiet(0.1)


def test_reloader_skips_missing_paths_when_configured(make_fixture, tmp_path):
    second = os.path.join(str(tmp_path), 'second.json')
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader = Reloader([f.path, second], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, skip_missing_paths=True, skip_unchanged=True)

    # Step 1: one file exists and one does not. The reload succeeds with the existing file.
    f.reloader.reload_now()
    result = f.require_applied()
    assert list(result.flags.keys()) == ['flag1']
    assert result.files == [FileSummary(path=f.path, present=True, flags=1), FileSummary(path=second, present=False)]

    # Step 2: the missing file appears. Its data is merged in.
    write_file(second, '{"flagValues": {"flag2": true}}')
    f.reloader.reload_now()
    result = f.require_applied()
    assert list(result.flags.keys()) == ['flag1', 'flag2']

    # Step 3: the file is deleted. Its data is gone and the reload still succeeds.
    os.remove(second)
    f.reloader.reload_now()
    result = f.require_applied()
    assert list(result.flags.keys()) == ['flag1']
    f.require_quiet(0.1)


def test_reloader_reports_failure_and_applies_nothing(make_fixture):
    f = make_fixture('{"flagValues"')
    f.reloader.reload_now()
    err = f.require_errored()
    assert isinstance(err, FileReadError)
    f.require_quiet(0.1)


def test_reloader_reports_merge_failure(make_fixture, tmp_path):
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(second, '{"flagValues": {"flag1": "dup"}}')
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader = Reloader([f.path, second], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    err = f.require_errored()
    assert isinstance(err, DuplicateKeyError)


def test_reloader_debounce_coalesces_triggers(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}', debounce_delay=0.2)
    for _ in range(20):
        f.reloader.trigger()
    f.require_applied()
    f.require_quiet()


def test_reloader_without_debounce_reloads_on_each_trigger(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.trigger()
    f.require_applied()
    f.reloader.trigger()
    f.require_applied()


def test_reloader_debounce_window_is_extended_by_each_trigger(make_fixture):
    # The debounce is a settle window: each trigger moves the deadline out again. A stream
    # of triggers spaced closer together than the window must produce no reload while the
    # stream continues, and exactly one reload after it stops.
    window = 0.25
    f = make_fixture('{"flagValues": {"flag1": true}}', debounce_delay=window)
    stop = time.time() + 5 * window
    while time.time() < stop:
        f.reloader.trigger()
        time.sleep(window / 5)
    assert f.applied.empty(), "a reload ran while triggers were still arriving"
    f.require_applied()
    f.require_quiet(2 * window)


def test_reloader_reports_identical_failure_only_once(make_fixture):
    f = make_fixture('{"flagValues"', retry_delay=SHORT_DELAY)
    f.reloader.reload_now()
    f.require_errored()
    # The automatic retries keep failing in the same way. They do not report again.
    f.require_quiet()

    # A different failure is reported.
    f.write('{"flags": {"flag1": "not an object"}}')
    f.require_errored()
    f.require_quiet()


def test_reloader_unused_spawns_no_thread(make_fixture):
    before = threading.active_count()
    f = make_fixture('{"flagValues": {"flag1": true}}')
    assert threading.active_count() == before
    f.reloader.reload_now()
    assert threading.active_count() == before + 1
    f.require_applied()


def test_reloader_close_does_not_wait_for_in_flight_reload(make_fixture):
    entered = threading.Event()
    release = threading.Event()

    def blocking_apply(result: MergeResult) -> None:
        entered.set()
        # The callback stays parked for far longer than close is given, so a close that waits
        # for the in-flight reload is detected as a failure rather than hidden by this timeout.
        release.wait(TEST_TIMEOUT * 6)

    f = make_fixture('{"flagValues": {"flag1": true}}', apply=blocking_apply)
    f.reloader.trigger()
    assert entered.wait(TEST_TIMEOUT), "the reload did not start"

    closed = threading.Event()

    def do_close():
        f.reloader.close()
        closed.set()

    threading.Thread(target=do_close, daemon=True).start()
    assert closed.wait(2.0), "close blocked on an in-flight reload"
    release.set()


def test_reloader_worker_thread_exits_after_close(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()
    worker = [t for t in threading.enumerate() if t.name == 'ldclient.filedata.reloader']
    assert len(worker) == 1
    f.reloader.close()
    worker[0].join(TEST_TIMEOUT)
    assert not worker[0].is_alive()


def test_reloader_retries_after_failure_without_further_triggers(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}', retry_delay=SHORT_DELAY)
    f.reloader.reload_now()
    f.require_applied()

    f.write('{"flagValues"')
    f.reloader.trigger()
    f.require_errored()

    # Fix the file without triggering. Only the automatic retry can observe the fix.
    f.write('{"flagValues": {"flag1": false}}')
    f.require_applied()


def test_reloader_retries_after_failed_initial_load(make_fixture):
    f = make_fixture('{"flagValues"', retry_delay=SHORT_DELAY)
    f.reloader.reload_now()
    f.require_errored()
    f.write('{"flagValues": {"flag1": true}}')
    f.require_applied()


def test_reloader_stops_retrying_after_success(make_fixture):
    f = make_fixture('{"flagValues"', retry_delay=SHORT_DELAY, skip_unchanged=True)
    f.reloader.reload_now()
    f.require_errored()

    f.write('{"flagValues": {"flag1": true}}')
    f.require_applied()

    # After the successful reload there are no further attempts: a changed file is not
    # picked up without a trigger.
    f.write('{"flagValues": {"flag1": false}}')
    f.require_quiet()


def test_reloader_does_not_retry_when_retry_delay_is_zero(make_fixture):
    f = make_fixture('{"flagValues"')
    f.reloader.reload_now()
    f.require_errored()
    f.write('{"flagValues": {"flag1": true}}')
    f.require_quiet()


def test_reloader_skip_unchanged(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}', skip_unchanged=True)
    f.reloader.reload_now()
    f.require_applied()

    f.reloader.trigger()
    f.require_quiet()

    f.write('{"flagValues": {"flag1": false}}')
    f.reloader.trigger()
    f.require_applied()


def test_reloader_recovery_applies_even_when_content_unchanged(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}', skip_unchanged=True)
    f.reloader.reload_now()
    f.require_applied()

    # A reload fails. Consumers hear about it and may move to an interrupted state.
    os.remove(f.path)
    f.reloader.trigger()
    f.require_errored()

    # The file comes back with byte-identical content. The success is applied despite
    # skip_unchanged, because only an application tells the consumer the interruption is over.
    f.write('{"flagValues": {"flag1": true}}')
    f.reloader.trigger()
    f.require_applied()

    # Once recovered, identical content skips again.
    f.reloader.trigger()
    f.require_quiet()


def test_reloader_applies_every_reload_when_skip_unchanged_is_off(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()
    f.reloader.trigger()
    f.require_applied()


def test_reloader_merges_multiple_files_in_order(make_fixture, tmp_path):
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(second, '{"flagValues": {"flag1": "second"}}')
    f = make_fixture('{"flagValues": {"flag1": "first"}}')
    f.reloader = Reloader([f.path, second], DuplicateKeysHandling.IGNORE, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    result = f.require_applied()
    assert result.flags['flag1'].variations == ['first']


def test_reloader_does_nothing_after_close(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()
    f.reloader.close()
    f.reloader.close()
    f.reloader.trigger()
    f.reloader.reload_now()
    f.require_quiet(0.1)


def test_reloader_serializes_reload_now_against_worker_reloads(make_fixture):
    # Concurrent reload_now calls and triggers must not interleave: every application is a
    # complete merged result, and the callback never runs on two threads at once.
    in_apply = threading.Lock()
    overlaps: List[bool] = []

    def apply(result: MergeResult) -> None:
        acquired = in_apply.acquire(blocking=False)
        overlaps.append(not acquired)
        try:
            assert list(result.flags.keys()) == ['flag1']
            time.sleep(0.002)
        finally:
            if acquired:
                in_apply.release()

    f = make_fixture('{"flagValues": {"flag1": true}}', apply=apply)
    threads = [threading.Thread(target=lambda: [f.reloader.reload_now() for _ in range(20)]) for _ in range(4)]
    for t in threads:
        t.start()
    for _ in range(20):
        f.reloader.trigger()
    for t in threads:
        t.join(TEST_TIMEOUT)
    f.reloader.close()
    assert not any(overlaps)


def test_reloader_survives_an_apply_callback_that_raises(make_fixture):
    calls: Queue = Queue()

    def apply(result: MergeResult) -> None:
        calls.put(result)
        raise RuntimeError("consumer failure")

    f = make_fixture('{"flagValues": {"flag1": true}}', apply=apply)
    f.reloader.trigger()
    take(calls)
    f.reloader.trigger()
    take(calls)


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------

POLL_INTERVAL = 0.05


class PollerFixture:
    def __init__(self, paths: List[str]):
        self.changes: Queue = Queue()
        self.poller = Poller(paths, POLL_INTERVAL, lambda: self.changes.put(True))
        self.poller.start()

    def require_change(self) -> None:
        take(self.changes)

    def require_no_change(self, duration: float = QUIET_PERIOD) -> None:
        require_quiet(self.changes, duration)


@pytest.fixture
def make_poller():
    pollers: List[PollerFixture] = []

    def factory(paths: List[str]) -> PollerFixture:
        fixture = PollerFixture(paths)
        pollers.append(fixture)
        return fixture

    yield factory
    for fixture in pollers:
        fixture.poller.close()


def set_mtime(path: str, seconds: float) -> None:
    os.utime(path, (seconds, seconds))


def test_poller_detects_modification(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    p = make_poller([path])
    p.require_no_change(0.15)
    write_file(path, 'bb')
    p.require_change()


def test_poller_detects_same_size_rewrite_with_new_mtime(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'aaa')
    set_mtime(path, 1000000)
    p = make_poller([path])
    write_file(path, 'bbb')
    set_mtime(path, 2000000)
    p.require_change()


def test_poller_detects_size_change_with_same_mtime(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'aaa')
    set_mtime(path, 1000000)
    p = make_poller([path])
    write_file(path, 'aaaa')
    set_mtime(path, 1000000)
    p.require_change()


def test_poller_fires_once_per_change(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    p = make_poller([path])
    write_file(path, 'bb')
    p.require_change()
    p.require_no_change()


def test_poller_detects_file_appearing(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    p = make_poller([path])
    p.require_no_change(0.15)
    write_file(path, 'a')
    p.require_change()


def test_poller_detects_file_disappearing(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    p = make_poller([path])
    os.remove(path)
    p.require_change()


def test_poller_watches_all_files(tmp_path, make_poller):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, 'a')
    write_file(second, 'a')
    p = make_poller([first, second])
    write_file(second, 'bb')
    p.require_change()
    write_file(first, 'bb')
    p.require_change()


def test_poller_stops_on_close(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    p = make_poller([path])
    p.poller.close()
    time.sleep(POLL_INTERVAL * 3)
    write_file(path, 'bb')
    p.require_no_change()


def test_poller_close_returns_while_callback_blocks(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    entered = threading.Event()
    release = threading.Event()

    def on_change():
        entered.set()
        release.wait(TEST_TIMEOUT)

    poller = Poller([path], POLL_INTERVAL, on_change)
    poller.start()
    write_file(path, 'bb')
    assert entered.wait(TEST_TIMEOUT)
    closed = threading.Event()

    def do_close():
        poller.close()
        closed.set()

    threading.Thread(target=do_close, daemon=True).start()
    assert closed.wait(TEST_TIMEOUT), "close blocked on the callback"
    release.set()


# ---------------------------------------------------------------------------
# Watcher
# ---------------------------------------------------------------------------

watchdog_required = pytest.mark.skipif(not have_watchdog, reason="watchdog is not installed")


class WatcherFixture:
    def __init__(self, paths: List[str]):
        self.changes: Queue = Queue()
        self.watcher = Watcher(paths, lambda: self.changes.put(True))

    def require_change(self) -> None:
        take(self.changes)

    def require_no_change(self, duration: float = QUIET_PERIOD) -> None:
        require_quiet(self.changes, duration)

    def drain(self) -> None:
        while True:
            try:
                self.changes.get(timeout=0.2)
            except Empty:
                return


@pytest.fixture
def make_watcher():
    watchers: List[WatcherFixture] = []

    def factory(paths: List[str]) -> WatcherFixture:
        fixture = WatcherFixture(paths)
        watchers.append(fixture)
        return fixture

    yield factory
    for fixture in watchers:
        fixture.watcher.close()


@watchdog_required
def test_watcher_detects_modification(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    w.require_no_change(0.15)
    write_file(path, 'bb')
    w.require_change()


@watchdog_required
def test_watcher_ignores_other_files_in_the_directory(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    other = os.path.join(str(tmp_path), 'other.json')
    write_file(path, 'a')
    w = make_watcher([path])
    write_file(other, 'bb')
    w.require_no_change()


@watchdog_required
def test_watcher_detects_absent_file_appearing(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    w = make_watcher([path])
    w.require_no_change(0.15)
    write_file(path, 'a')
    w.require_change()


@watchdog_required
def test_watcher_detects_file_written_by_rename(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    temp = os.path.join(str(tmp_path), 'data.json.tmp')
    write_file(path, 'a')
    w = make_watcher([path])
    write_file(temp, 'bb')
    w.drain()
    os.replace(temp, path)
    w.require_change()


@watchdog_required
def test_watcher_matches_the_destination_of_a_move_event(tmp_path, make_watcher):
    # A file written by rename arrives as a move event whose destination is the watched path.
    import watchdog.events

    path = os.path.join(str(tmp_path), 'data.json')
    temp = os.path.join(str(tmp_path), 'data.json.tmp')
    real_path = os.path.join(os.path.realpath(str(tmp_path)), 'data.json')
    real_temp = os.path.join(os.path.realpath(str(tmp_path)), 'data.json.tmp')
    w = make_watcher([path])
    w.watcher._handle_event(watchdog.events.FileMovedEvent(real_temp, real_path))
    w.require_change()
    w.watcher._handle_event(watchdog.events.FileMovedEvent(real_path, real_temp))
    w.require_change()
    w.watcher._handle_event(watchdog.events.FileMovedEvent(real_temp, real_temp + '.other'))
    w.require_no_change()
    assert temp not in w.watcher._watched_paths


@watchdog_required
def test_watcher_ignores_events_that_do_not_change_the_file(tmp_path, make_watcher):
    # Opening and reading a watched file produces notifications too. A reload reads the files,
    # so reacting to those would make every reload trigger the next one.
    import watchdog.events

    path = os.path.join(str(tmp_path), 'data.json')
    real_path = os.path.join(os.path.realpath(str(tmp_path)), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    w.watcher._handle_event(watchdog.events.FileOpenedEvent(real_path))
    w.watcher._handle_event(watchdog.events.FileClosedNoWriteEvent(real_path))
    w.require_no_change()
    w.watcher._handle_event(watchdog.events.FileClosedEvent(real_path))
    w.require_change()


@watchdog_required
def test_watcher_does_not_signal_when_the_file_is_only_read(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    for _ in range(3):
        with open(path, 'rb') as f:
            f.read()
    w.require_no_change()


@watchdog_required
def test_watcher_detects_file_deletion(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    os.remove(path)
    w.require_change()


@watchdog_required
def test_watcher_picks_up_directory_that_appears_later(tmp_path, make_watcher):
    directory = os.path.join(str(tmp_path), 'later')
    path = os.path.join(directory, 'data.json')
    w = make_watcher([path])
    w.require_no_change(0.15)
    os.mkdir(directory)
    write_file(path, 'a')
    # The retry that watches the new directory signals a change so the file is read.
    w.require_change()
    w.drain()
    write_file(path, 'bb')
    w.require_change()


@watchdog_required
def test_watcher_close_stops_notifications(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    w.watcher.close()
    write_file(path, 'bb')
    w.require_no_change()
