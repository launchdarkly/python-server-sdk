import logging
import os
import threading
import time
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest import mock

import pytest

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from ldclient.impl.integrations.files import filedata
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
    is_yaml_path,
    load_files,
    make_flag_with_value,
    merge,
    parse_document,
    read_file
)
from ldclient.impl.model import FeatureFlag, Segment
from ldclient.testing.file_util import rewrite_in_place, truncate

TEST_TIMEOUT = 5.0

# Timings for the reloader and poller tests. They are generous multiples of the configured
# delays so the tests stay deterministic on a loaded machine.
SHORT_DELAY = 0.05
QUIET_PERIOD = 0.3

# The pause of the fixture's in-place rewrites, between truncating the file and writing it.
REWRITE_PAUSE = 0.03


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


@pytest.mark.parametrize('path', ['/data.yaml', '/data.yml', '/data.YAML', '/data.Yml', 'relative/data.yaml', '/x/.yaml', '/x/.YML'])
def test_a_yaml_extension_selects_yaml(path):
    assert is_yaml_path(path)


@pytest.mark.parametrize('path', ['/data.json', '/data', '/data.txt', '/data.yaml.json', '/yaml', '/data.yaml/file', '/data.yamlx'])
def test_any_other_file_is_json(path):
    assert not is_yaml_path(path)


def test_parse_chooses_the_parser_by_the_extension_not_the_content():
    # A tab inside a string is valid YAML but not valid JSON. Installing pyyaml does not make a
    # JSON file YAML: the extension decides, whatever the content looks like.
    content = b'  \n {"flagValues": {"flag1": "a\tb"}}'
    with pytest.raises(ValueError):
        parse_document(content)
    with pytest.raises(ValueError):
        parse_document(b'flagValues:\n  flag1: true\n')
    if have_yaml:
        assert parse_document(content, is_yaml=True).flag_values == {'flag1': 'a\tb'}


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_parse_yaml_document():
    document = parse_document(b'---\nflags:\n  flag1:\n    key: flag1\n    "on": true\nflagValues:\n  flag2: value2\nsegments:\n  seg1:\n    key: seg1\n', is_yaml=True)
    assert document.flags['flag1'].on is True
    assert document.flag_values == {'flag2': 'value2'}
    assert list(document.segments.keys()) == ['seg1']


def test_parse_yaml_without_pyyaml_fails(monkeypatch):
    monkeypatch.setattr(filedata, 'have_yaml', False)
    with pytest.raises(ValueError) as excinfo:
        parse_document(b'flagValues:\n  flag1: true\n', is_yaml=True)
    assert 'pyyaml' in str(excinfo.value)


@pytest.mark.parametrize('content', [b'', b'  \n\t\r\n', b'\xef\xbb\xbf', b'\xef\xbb\xbf \n'], ids=['empty', 'white-space', 'byte-order-mark', 'byte-order-mark-and-white-space'])
def test_parse_empty_json_is_not_a_document(content):
    # A file that is rewritten in place is empty for a moment. As JSON it is invalid, so the load
    # fails and the last good data stays.
    with pytest.raises(ValueError) as excinfo:
        parse_document(content)
    assert str(excinfo.value) == 'the file has no content, which is not a JSON document'


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
@pytest.mark.parametrize('content', [b'', b'  \n\n', b'---\n', b'# no entries yet\n', b'\xef\xbb\xbf'], ids=['empty', 'white-space', 'document-start', 'comment', 'byte-order-mark'])
def test_parse_empty_yaml_has_no_entries(content):
    assert parse_document(content, is_yaml=True) == Document()


def test_parse_json_object_with_no_members_has_no_entries():
    assert parse_document(b'{}') == Document()
    assert parse_document(b' {} \n') == Document()


def test_parse_ignores_a_byte_order_mark():
    assert parse_document(b'\xef\xbb\xbf{"flagValues": {"flag1": true}}').flag_values == {'flag1': True}
    if have_yaml:
        assert parse_document(b'\xef\xbb\xbfflagValues:\n  flag1: true\n', is_yaml=True).flag_values == {'flag1': True}


def test_parse_fills_in_missing_key_and_version():
    document = parse_document(b'{"flags": {"flag1": {"on": true}}, "segments": {"seg1": {}}}')
    assert document.flags['flag1'].key == 'flag1'
    assert document.flags['flag1'].version == 1
    assert document.segments['seg1'].key == 'seg1'
    assert document.segments['seg1'].version == 1


@pytest.mark.parametrize('content', [b'null', b' null\n', b'["a", "b"]', b'1', b'"text"', b'true'])
def test_parse_rejects_a_json_document_that_is_not_an_object(content):
    with pytest.raises(ValueError) as excinfo:
        parse_document(content)
    assert str(excinfo.value) == 'the document must be an object'


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_parse_reads_yaml_null_as_no_entries():
    assert parse_document(b'null\n', is_yaml=True) == Document()
    assert parse_document(b'~\n', is_yaml=True) == Document()


def test_parse_rejects_documents_that_are_not_objects():
    if have_yaml:
        with pytest.raises(ValueError):
            parse_document(b'- a\n- b\n', is_yaml=True)
    with pytest.raises(ValueError):
        parse_document(b'["a", "b"]')
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


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_parse_rejects_malformed_yaml():
    with pytest.raises(yaml.YAMLError):
        parse_document(b'flagValues: [', is_yaml=True)


def test_parse_validates_definition_property_types():
    with pytest.raises(ValueError):
        parse_document(b'{"flags": {"flag1": {"key": "flag1", "version": "not a number"}}}')
    with pytest.raises(ValueError):
        parse_document(b'{"segments": {"seg1": {"key": "seg1", "version": 1, "included": "not a list"}}}')


@pytest.mark.parametrize('content', [
    '{"flagValues": {"flag1": true}}'.encode('utf-16'),
    '{"flagValues": {"flag1": true}}'.encode('utf-32'),
    b'{"flagValues": {"flag1": "\xff"}}',
], ids=['utf-16', 'utf-32', 'invalid-utf-8'])
@pytest.mark.parametrize('is_yaml', [False, pytest.param(True, marks=pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed"))], ids=['json', 'yaml'])
def test_parse_rejects_content_that_is_not_utf_8(content, is_yaml):
    with pytest.raises(ValueError):
        parse_document(content, is_yaml=is_yaml)


def test_reloader_reports_a_file_that_is_not_utf_8_and_keeps_working(make_fixture):
    # A file in another encoding fails to load like any other content that cannot be parsed, and
    # the reloader keeps loading the files when they change.
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()
    with open(f.path, 'wb') as out:
        out.write('{"flagValues": {"flag1": false}}'.encode('utf-16'))
    f.reloader.trigger()
    assert isinstance(f.require_errored(), FileReadError)
    write_file(f.path, '{"flagValues": {"flag1": false}}')
    f.reloader.trigger()
    assert f.require_applied().flags['flag1'].variations == [False]


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

    write_file(path, '')
    with pytest.raises(FileReadError) as excinfo:
        read_file(path)
    assert 'no content' in str(excinfo.value)


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_read_file_yaml(tmp_path):
    path = os.path.join(str(tmp_path), 'data.yaml')
    write_file(path, 'flagValues:\n  flag1: true\n')
    assert read_file(path).flag_values == {'flag1': True}
    write_file(path, '')
    assert read_file(path) == Document()


def test_abs_file_paths():
    paths = abs_file_paths(['relative/data.json', '/absolute/data.json'])
    assert paths[0] == os.path.abspath('relative/data.json')
    assert paths[1] == os.path.abspath('/absolute/data.json')


def test_make_flag_with_value_is_on_and_serves_the_value_by_fallthrough():
    flag = make_flag_with_value('flag1', 'value1')
    assert flag.key == 'flag1'
    assert flag.version == 1
    assert flag.on is True
    assert flag.off_variation is None
    assert flag.fallthrough.variation == 0
    assert flag.variations == ['value1']
    assert flag.to_json_dict() == {'key': 'flag1', 'version': 1, 'on': True, 'fallthrough': {'variation': 0}, 'variations': ['value1']}


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
    assert result.flags['flag2'].on is True
    assert result.flags['flag2'].fallthrough.variation == 0


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


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_load_files_parses_each_file_in_the_format_of_its_extension(tmp_path):
    json_path = os.path.join(str(tmp_path), 'first.json')
    yaml_path = os.path.join(str(tmp_path), 'second.YML')
    write_file(json_path, '{"flagValues": {"flag1": "json"}}')
    write_file(yaml_path, 'flagValues:\n  flag2: yaml\n')
    result = load_files([json_path, yaml_path], DuplicateKeysHandling.FAIL)
    assert {key: flag.variations for key, flag in result.flags.items()} == {'flag1': ['json'], 'flag2': ['yaml']}

    # The same YAML content in a file without a YAML extension is not JSON.
    write_file(json_path, 'flagValues:\n  flag1: json\n')
    with pytest.raises(FileReadError) as excinfo:
        load_files([json_path, yaml_path], DuplicateKeysHandling.FAIL)
    assert excinfo.value.path == json_path


@pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")
def test_load_files_reads_an_empty_yaml_file_as_no_entries_and_fails_on_an_empty_json_file(tmp_path):
    json_path = os.path.join(str(tmp_path), 'data.json')
    yaml_path = os.path.join(str(tmp_path), 'data.yaml')
    write_file(json_path, '{"flagValues": {"flag1": true}}')
    write_file(yaml_path, '')
    result = load_files([json_path, yaml_path], DuplicateKeysHandling.FAIL)
    assert list(result.flags.keys()) == ['flag1']
    assert result.files[1] == FileSummary(path=yaml_path, present=True)

    write_file(json_path, '')
    with pytest.raises(FileReadError) as excinfo:
        load_files([json_path, yaml_path], DuplicateKeysHandling.FAIL)
    assert excinfo.value.path == json_path
    assert 'no content' in str(excinfo.value)


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
    def __init__(self, tmp_path, initial_content: str, name: str = 'data.json', **kwargs):
        self.path = os.path.join(str(tmp_path), name)
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
        # Applications rewrite data files in place, which is not atomic. The fixture does too,
        # deliberately slowly: the file is truncated and written a little later, so a load can
        # find it empty, as it would with an application's writes, instead of only by chance.
        rewrite_in_place(self.path, content, REWRITE_PAUSE)

    def require_applied(self, timeout: float = TEST_TIMEOUT) -> MergeResult:
        return take(self.applied, timeout)

    def require_errored(self, timeout: float = TEST_TIMEOUT) -> Exception:
        return take(self.errored, timeout)

    def require_quiet(self, duration: float = QUIET_PERIOD) -> None:
        deadline = time.time() + duration
        while time.time() < deadline:
            if not self.applied.empty():
                pytest.fail("unexpected apply call")
            if not self.errored.empty():
                pytest.fail("unexpected on_error call")
            time.sleep(0.01)

    def drain(self, duration: float = QUIET_PERIOD) -> Tuple[List[MergeResult], List[Exception]]:
        """Collects every apply and on_error call that arrives within the duration."""
        applied: List[MergeResult] = []
        errored: List[Exception] = []
        deadline = time.time() + duration
        while time.time() < deadline:
            while not self.applied.empty():
                applied.append(self.applied.get_nowait())
            while not self.errored.empty():
                errored.append(self.errored.get_nowait())
            time.sleep(0.01)
        return applied, errored

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

    # A different failure is reported. The write is in place, so a retry can run while the file
    # is empty. That retry reads the file again instead of reporting it, and a retry that finds
    # it still empty reports that it has no content. The final content is loaded on demand, and
    # every report is checked: each one is the new failure or the empty file, the new failure is
    # among them, and once it is the current failure, the retries that repeat it stay silent.
    f.write('{"flags": {"flag1": "not an object"}}')
    f.reloader.reload_now()
    applied, errored = f.drain()
    assert applied == []
    assert any('must be an object' in str(e) for e in errored), errored
    assert all('must be an object' in str(e) or 'no content' in str(e) for e in errored), errored
    f.require_quiet()


def reloader_threads() -> Set[threading.Thread]:
    return {t for t in threading.enumerate() if t.name == 'ldclient.filedata.reloader'}


def test_reloader_unused_spawns_no_thread(make_fixture):
    before = reloader_threads()
    f = make_fixture('{"flagValues": {"flag1": true}}')
    assert reloader_threads() == before
    f.reloader.reload_now()
    assert len(reloader_threads() - before) == 1
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
    before = reloader_threads()
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()
    started = reloader_threads() - before
    assert len(started) == 1
    f.reloader.close()
    worker = started.pop()
    worker.join(TEST_TIMEOUT)
    assert not worker.is_alive()


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
    # The retry delay is well over the pause of the fixture's in-place writes, so that a retry is
    # unlikely to read the file in the middle of one on a slow machine.
    f = make_fixture('{"flagValues"', retry_delay=0.2, skip_unchanged=True)
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
    applied_keys: List[List[str]] = []

    def apply(result: MergeResult) -> None:
        acquired = in_apply.acquire(blocking=False)
        overlaps.append(not acquired)
        try:
            applied_keys.append(list(result.flags.keys()))
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
    assert len(applied_keys) > 0
    assert all(keys == ['flag1'] for keys in applied_keys)


def test_reloader_reports_an_apply_failure_and_retries_it(make_fixture):
    # The consumer rejects every result until it is released. Each call records whether the
    # result was accepted.
    accept = threading.Event()
    calls: Queue = Queue()

    def apply(result: MergeResult) -> None:
        accepted = accept.is_set()
        calls.put(accepted)
        if not accepted:
            raise RuntimeError("consumer failure")

    f = make_fixture('{"flagValues": {"flag1": true}}', apply=apply, retry_delay=SHORT_DELAY, skip_unchanged=True)

    # The synchronous load returns normally. The rejection is reported like a load failure.
    f.reloader.reload_now()
    assert take(calls) is False
    assert isinstance(f.require_errored(), RuntimeError)

    # The automatic retry offers the unchanged content again until the consumer accepts it.
    accept.set()
    while take(calls) is False:
        pass

    # Once the consumer has accepted the result, the retries stop.
    with pytest.raises(Empty):
        calls.get(timeout=QUIET_PERIOD)


def test_reloader_retries_when_the_error_callback_raises(make_fixture):
    def on_error(err: Exception) -> None:
        raise RuntimeError("reporter failure")

    f = make_fixture('{"flagValues"', on_error=on_error, retry_delay=SHORT_DELAY)

    # The failing report does not escape the synchronous load.
    f.reloader.reload_now()

    # The automatic retry still runs and picks up the corrected file.
    f.write('{"flagValues": {"flag1": true}}')
    f.require_applied()


# ---------------------------------------------------------------------------
# Reloader: files that are found empty
# ---------------------------------------------------------------------------

yaml_required = pytest.mark.skipif(not have_yaml, reason="pyyaml is not installed")

# The retry delay of the tests of a file that is found empty. The retry comes long enough after a
# trigger for a test to finish rewriting the file before it, even on a slow machine.
EMPTY_FILE_RETRY_DELAY = 1.5

# Shorter than EMPTY_FILE_RETRY_DELAY, so a load that happens within it after a trigger is not
# the retry.
BEFORE_THE_RETRY = 1.0

DEFERRAL_LOG = 'waiting for it to be written'

YAML_FLAG1 = 'flagValues:\n  flag1: true\n'
YAML_FLAG2 = 'flagValues:\n  flag2: true\n'


def flag_keys(result: MergeResult) -> List[str]:
    return list(result.flags.keys())


def deferrals_logged(caplog) -> int:
    return len([r for r in caplog.records if r.levelno == logging.DEBUG and DEFERRAL_LOG in r.getMessage()])


def wait_for_deferrals(caplog, count: int) -> None:
    deadline = time.monotonic() + TEST_TIMEOUT
    while deferrals_logged(caplog) < count:
        assert time.monotonic() < deadline, "the empty file was not held back"
        time.sleep(0.01)


def change_modification_time(path: str) -> None:
    """
    Makes sure that the file's modification time differs from what it was, as a rewrite changes
    it, even on a file system whose timestamps are too coarse to tell two quick writes apart.
    """
    modified = os.stat(path).st_mtime_ns + 1000000
    os.utime(path, ns=(modified, modified))


class FakeClock:
    """A monotonic clock that only moves when a test advances it."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# The retry delay of the tests that time a hold on a fake clock. The timers use the real clock,
# so no retry fires during such a test, and each load is started with reload_now.
HOUR = 3600.0


def make_held_fixture(make_fixture, content: str, name: str = 'data.yaml', **kwargs) -> Tuple[ReloaderFixture, FakeClock]:
    clock = FakeClock()
    f = make_fixture(content, name=name, retry_delay=HOUR, clock=clock, **kwargs)
    f.reloader.reload_now()
    f.require_applied()
    return f, clock


def make_two_file_fixture(make_fixture, tmp_path, first: str, second: str, **kwargs) -> Tuple[ReloaderFixture, FakeClock, str]:
    clock = FakeClock()
    other = os.path.join(str(tmp_path), 'other.yaml')
    write_file(other, second)
    f = make_fixture(first, name='data.yaml')
    f.reloader = Reloader([f.path, other], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock, **kwargs)
    f.reloader.reload_now()
    f.require_applied()
    return f, clock, other


def load(f: ReloaderFixture) -> Optional[MergeResult]:
    """Loads the files at once, and returns what was applied, or None when nothing was."""
    f.reloader.reload_now()
    try:
        return f.applied.get_nowait()
    except Empty:
        return None


def require_held_back(f: ReloaderFixture) -> None:
    assert load(f) is None, "the load applied the file instead of holding it back"
    assert f.errored.empty(), "the load reported %r" % f.errored.get_nowait()


@yaml_required
def test_reloader_holds_back_a_file_until_it_has_stayed_empty_for_exactly_the_retry_delay(make_fixture, caplog):
    # A writer that rewrites the file in place truncates it and then writes the new content. A
    # load that finds the file empty while its content is in effect keeps the data. In YAML an
    # empty file is valid, so applying it would briefly remove every entry.
    caplog.set_level(logging.DEBUG)
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    assert deferrals_logged(caplog) == 1

    clock.advance(0.95 * HOUR)
    require_held_back(f)

    clock.advance(0.05 * HOUR)
    result = load(f)
    assert result is not None and result.flags == {}


@yaml_required
def test_reloader_restarts_the_delay_when_the_held_back_file_is_written(make_fixture):
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)

    # Rewritten and truncated again between two loads: its state changed, so it is a new rewrite.
    clock.advance(0.6 * HOUR)
    write_file(f.path, YAML_FLAG2)
    truncate(f.path)
    change_modification_time(f.path)
    require_held_back(f)

    clock.advance(0.6 * HOUR)
    require_held_back(f)

    clock.advance(0.4 * HOUR)
    result = load(f)
    assert result is not None and result.flags == {}


@yaml_required
def test_reloader_observes_a_held_back_file_through_a_symbolic_link(make_fixture, tmp_path):
    # The configured path is a link to the file that is rewritten, as with a mounted ConfigMap.
    # Writing the file changes its state as seen through the link, so the delay starts over.
    target = os.path.join(str(tmp_path), 'target.yaml')
    link = os.path.join(str(tmp_path), 'link.yaml')
    write_file(target, YAML_FLAG1)
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError):
        pytest.skip("symbolic links are not available")
    clock = FakeClock()
    f = make_fixture(YAML_FLAG1, name='unused.yaml')
    f.reloader = Reloader([link], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    truncate(target)
    require_held_back(f)
    clock.advance(0.6 * HOUR)
    write_file(target, YAML_FLAG2)
    truncate(target)
    change_modification_time(target)
    require_held_back(f)
    clock.advance(0.6 * HOUR)
    require_held_back(f)


@yaml_required
def test_reloader_forgets_an_emptying_once_the_file_is_read_with_content(make_fixture):
    # A file that was held back, then applied with content, and emptied again much later starts a
    # new delay, even if it ends up with the same modification time as at the first emptying, as
    # when a tool restores the times of the files it writes.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    first = os.stat(f.path)
    require_held_back(f)
    write_file(f.path, YAML_FLAG2)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']

    clock.advance(10 * HOUR)
    truncate(f.path)
    os.utime(f.path, ns=(first.st_atime_ns, first.st_mtime_ns))
    require_held_back(f)


@yaml_required
def test_reloader_holds_back_a_file_emptied_again_after_a_read_with_content_whose_state_looks_the_same(make_fixture, tmp_path):
    # On a file system that records modification times coarsely, a file emptied a second time can
    # look exactly as it did the first time. The read of its content in between still starts its
    # delay over. The other file is kept written while empty, so that no load applies.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    first_emptying = os.stat(f.path)
    truncate(other)
    require_held_back(f)

    clock.advance(50 / 60 * HOUR)
    write_file(f.path, YAML_FLAG1)
    change_modification_time(other)
    require_held_back(f)

    clock.advance(20 / 60 * HOUR)
    truncate(f.path)
    os.utime(f.path, ns=(first_emptying.st_atime_ns, first_emptying.st_mtime_ns))
    change_modification_time(other)
    require_held_back(f)

    # The other file gets its content back while this one is still within its new delay.
    clock.advance(10 / 60 * HOUR)
    write_file(other, YAML_FLAG2)
    require_held_back(f)


@yaml_required
def test_reloader_holds_back_a_file_rewritten_with_the_same_content_again_later(make_fixture):
    # A tool rewrites the file in place with the same content now and then. The load after each
    # rewrite is skipped as unchanged, which confirms the data in effect like an application, so
    # a later rewrite is held back on its own and not cut short by the first one's bound.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1, skip_unchanged=True)
    truncate(f.path)
    require_held_back(f)
    write_file(f.path, YAML_FLAG1)
    assert load(f) is None

    clock.advance(6 * HOUR)
    truncate(f.path)
    require_held_back(f)


@yaml_required
def test_reloader_holds_back_a_file_emptied_again_after_unparsable_content_whose_state_looks_the_same(make_fixture):
    # The file is emptied and held back, then saved with content that cannot be parsed, then
    # emptied again with the modification time of its first emptying, as a coarse file system can
    # make it look. Reading content ended its empty state, so it starts a new delay.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    first_emptying = os.stat(f.path)
    require_held_back(f)
    write_file(f.path, 'flagValues: [')
    assert load(f) is None
    assert isinstance(f.errored.get_nowait(), FileReadError)

    clock.advance(70 / 60 * HOUR)
    truncate(f.path)
    os.utime(f.path, ns=(first_emptying.st_atime_ns, first_emptying.st_mtime_ns))
    require_held_back(f)


@yaml_required
def test_reloader_applies_the_written_content_of_a_held_back_file_at_once(make_fixture):
    # Two quick rewrites in place: the second truncation follows the first write before any load
    # reads the new content, as when both fall in one debounce window. The file is empty at every
    # load, and its emptiness is not applied; the content that is finally written is.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    write_file(f.path, YAML_FLAG2)
    truncate(f.path)
    change_modification_time(f.path)
    clock.advance(0.2 * HOUR)
    require_held_back(f)

    write_file(f.path, 'flagValues:\n  flag3: true\n')
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag3']


@yaml_required
@pytest.mark.parametrize('content', ['  \n\n', '# no entries yet\n', '---\n', 'null\n'], ids=['white-space', 'comment', 'document-marker', 'null'])
def test_reloader_holds_back_a_yaml_file_that_has_no_entries(make_fixture, content):
    # A read can see only the start of a YAML file that is being written: white space, a comment
    # or a document marker, which the parser reads as nothing.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    write_file(f.path, content)
    require_held_back(f)
    write_file(f.path, YAML_FLAG2)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']


@yaml_required
def test_reloader_applies_a_yaml_object_with_no_members_at_once(make_fixture, caplog):
    # {} is a document with no entries, written on purpose to remove them, not part of a file.
    caplog.set_level(logging.DEBUG)
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    write_file(f.path, '{}\n')
    result = load(f)
    assert result is not None and result.flags == {}
    assert deferrals_logged(caplog) == 0


@yaml_required
def test_reloader_holds_back_files_emptied_together_for_one_retry_delay(make_fixture, tmp_path, caplog):
    # Every file is read and classified before any is held back, so two files that are emptied
    # together start their delays in the same load and are applied together.
    caplog.set_level(logging.DEBUG)
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    truncate(other)
    require_held_back(f)
    assert deferrals_logged(caplog) == 2

    clock.advance(HOUR)
    result = load(f)
    assert result is not None and result.flags == {}


@yaml_required
def test_reloader_holds_back_an_emptied_file_while_another_file_changes(make_fixture, tmp_path):
    # A change to one file starts a load while another is in the middle of a rewrite. The load
    # applies all files or none, so the change waits too.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    require_held_back(f)

    write_file(other, 'flagValues:\n  flag2: false\n')
    clock.advance(0.3 * HOUR)
    require_held_back(f)

    write_file(f.path, YAML_FLAG1)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag1', 'flag2']
    assert result.flags['flag2'].variations == [False]


def test_reloader_fails_on_a_file_that_cannot_be_parsed_while_another_is_held_back(make_fixture, tmp_path):
    clock = FakeClock()
    other = os.path.join(str(tmp_path), 'other.json')
    write_file(other, '{"flagValues": {"flag2": true}}')
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader = Reloader([f.path, other], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    # One file is mid-rewrite and the other is broken. The broken file is reported at once.
    truncate(f.path)
    write_file(other, '{"flagValues"')
    assert load(f) is None
    err = f.errored.get_nowait()
    assert isinstance(err, FileReadError)
    assert err.path == other


@yaml_required
def test_reloader_stops_holding_back_a_file_that_keeps_being_emptied_after_five_retry_delays(make_fixture):
    # Every load finds the file empty with a new modification time, so its delay keeps starting
    # over. The run of loads that hold it back ends after five retry delays.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    for _ in range(9):
        clock.advance(0.5 * HOUR)
        change_modification_time(f.path)
        require_held_back(f)

    clock.advance(0.5 * HOUR)
    change_modification_time(f.path)
    result = load(f)
    assert result is not None and result.flags == {}


@yaml_required
def test_reloader_stops_holding_back_files_rewritten_in_turn_after_five_retry_delays(make_fixture, tmp_path):
    # Two files are rewritten in turn, so every load finds one of them empty, and each is read
    # with content in between. The run of loads that hold files back still ends after five retry
    # delays.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    paths = [f.path, other]
    contents = [YAML_FLAG1, YAML_FLAG2]
    truncate(f.path)
    require_held_back(f)
    for i in range(1, 10):
        clock.advance(0.5 * HOUR)
        write_file(paths[(i - 1) % 2], contents[(i - 1) % 2])
        truncate(paths[i % 2])
        require_held_back(f)

    clock.advance(0.5 * HOUR)
    write_file(paths[1], contents[1])
    truncate(paths[0])
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']


@yaml_required
def test_reloader_measures_a_file_from_its_own_read_when_an_earlier_file_reads_slowly(make_fixture, tmp_path, monkeypatch):
    # The read of the earlier file stalls while the later file is being rewritten in place. The
    # later file's delay counts from when it was read, not from when the load began.
    clock = FakeClock()
    slow = os.path.join(str(tmp_path), 'slow.yaml')
    write_file(slow, YAML_FLAG2)
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([slow, f.path], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    original = filedata._read_file
    stall = [True]

    def read_file(path: str, skip_missing_paths: bool):
        read = original(path, skip_missing_paths)
        if path == slow and stall[0]:
            truncate(f.path)
            clock.advance(0.9 * HOUR)
        return read

    monkeypatch.setattr(filedata, '_read_file', read_file)
    require_held_back(f)
    stall[0] = False
    clock.advance(0.2 * HOUR)
    require_held_back(f)

    write_file(f.path, 'flagValues:\n  flag3: true\n')
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2', 'flag3']


@yaml_required
def test_reloader_measures_a_file_from_its_own_read_when_a_later_file_reads_slowly(make_fixture, tmp_path, monkeypatch):
    # The file is found empty before the read of a later file stalls. Its delay counts from its
    # own read, so it is over one retry delay after that read, and the retry the hold asks for
    # comes then.
    clock = FakeClock()
    slow = os.path.join(str(tmp_path), 'slow.yaml')
    write_file(slow, YAML_FLAG2)
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, slow], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    original = filedata._read_file
    stall = [True]

    def read_file(path: str, skip_missing_paths: bool):
        read = original(path, skip_missing_paths)
        if path == slow and stall[0]:
            clock.advance(0.9 * HOUR)
        return read

    monkeypatch.setattr(filedata, '_read_file', read_file)
    truncate(f.path)
    require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(0.1 * HOUR + 0.1 * HOUR)
    stall[0] = False
    clock.advance(0.1 * HOUR)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']


@yaml_required
def test_reloader_a_failing_other_file_does_not_restart_the_bound(make_fixture, tmp_path):
    # The file keeps being written while every load finds it empty, and the other file fails to
    # parse every other load. The failures do not start the file's bound over.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    require_held_back(f)
    for i in range(1, 10):
        clock.advance(0.5 * HOUR)
        change_modification_time(f.path)
        write_file(other, 'flagValues: [' if i % 2 == 1 else YAML_FLAG2)
        assert load(f) is None

    clock.advance(0.5 * HOUR)
    change_modification_time(f.path)
    write_file(other, YAML_FLAG2)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']


@yaml_required
def test_reloader_another_files_bound_does_not_cut_a_rewrite_short(make_fixture, tmp_path):
    # One file keeps being written while every load finds it empty, until its bound is over.
    # The other file is a quarter of a retry delay into an ordinary rewrite then, and stays held
    # back until it is written.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    require_held_back(f)
    for _ in range(9):
        clock.advance(0.5 * HOUR)
        change_modification_time(f.path)
        require_held_back(f)

    clock.advance(0.25 * HOUR)
    change_modification_time(f.path)
    truncate(other)
    require_held_back(f)

    clock.advance(0.25 * HOUR)
    change_modification_time(f.path)
    require_held_back(f)

    write_file(other, 'flagValues:\n  flag2: false\n')
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']
    assert result.flags['flag2'].variations == [False]


@yaml_required
def test_reloader_applies_files_emptied_in_staggered_turns_after_the_bound(make_fixture, tmp_path):
    # Two files stay empty and are truncated again in turn, so that whenever one of them has
    # stayed empty for the whole delay, the other is still within its delay and holds the load
    # back. A file that settles so does not get a fresh bound while the other is held back.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    require_held_back(f)
    applied = False
    for i in range(1, 15):
        clock.advance(54 / 60 * HOUR)
        if i == 1:
            truncate(other)
        change_modification_time(other if i % 2 == 1 else f.path)
        applied = load(f) is not None
        if applied:
            break
        # The retry that the hold arms, a little after the other file's delay is over.
        clock.advance(12 / 60 * HOUR)
        applied = load(f) is not None
        clock.advance(-12 / 60 * HOUR)
        if applied:
            break
    assert applied, "the files were held back without end"


@yaml_required
def test_reloader_bound_of_a_file_held_back_during_a_failure_starts_over_once_it_is_read_again(make_fixture, tmp_path):
    # While the other file fails to load, this file is rewritten once and its gap is held back.
    # Much later, the other file is fixed while this one is in the middle of another rewrite,
    # which is held back with a bound of its own.
    clock = FakeClock()
    broken = os.path.join(str(tmp_path), 'other.json')
    write_file(broken, '{"flagValues": {"flag3": false}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, broken], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    write_file(broken, '{bad')
    truncate(f.path)
    assert load(f) is None
    write_file(f.path, YAML_FLAG2)
    assert load(f) is None
    clock.advance(6 * HOUR)
    assert load(f) is None
    assert isinstance(f.errored.get_nowait(), FileReadError)
    assert f.errored.empty(), "an identical failure was reported again"

    write_file(broken, '{"flagValues": {"flag3": false}}')
    truncate(f.path)
    require_held_back(f)


@yaml_required
def test_reloader_hold_asks_for_the_retry_from_the_end_of_the_load(make_fixture, tmp_path, monkeypatch):
    # The read of the file after the held one takes longer than what is left of the held file's
    # delay, so the retry is due right away, after the margin.
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    truncate(f.path)
    require_held_back(f)

    original = filedata._read_file
    stall = [True]

    def read_file(path: str, skip_missing_paths: bool):
        read = original(path, skip_missing_paths)
        if path == other and stall[0]:
            stall[0] = False
            clock.advance(6 / 60 * HOUR)
        return read

    monkeypatch.setattr(filedata, '_read_file', read_file)
    clock.advance(57 / 60 * HOUR)
    require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(0.1 * HOUR)


@pytest.mark.parametrize('name, good, broken, fixed', [
    pytest.param('data.yaml', YAML_FLAG1, 'flagValues: [', 'flagValues:\n  flag1: false\n', marks=yaml_required, id='yaml'),
    pytest.param('data.json', '{"flagValues": {"flag1": true}}', '{"flagValues"', '{"flagValues": {"flag1": false}}', id='json'),
])
def test_reloader_holds_back_the_fix_of_a_file_saved_in_place_with_an_error(make_fixture, name, good, broken, fixed):
    # An operator saves a typo in place, and the fix in place much later. The truncation of the
    # second save is held back like the first, so the fix is applied and the file never empty.
    f, clock = make_held_fixture(make_fixture, good, name=name)
    truncate(f.path)
    require_held_back(f)
    write_file(f.path, broken)
    assert load(f) is None
    assert isinstance(f.errored.get_nowait(), FileReadError)
    for _ in range(6):
        clock.advance(HOUR)
        assert load(f) is None
    assert f.errored.empty(), "an identical failure was reported again"

    truncate(f.path)
    require_held_back(f)
    write_file(f.path, fixed)
    result = load(f)
    assert result is not None and result.flags['flag1'].variations == [False]


@yaml_required
def test_reloader_file_kept_written_while_empty_is_applied_after_its_bound_while_an_earlier_file_fails(make_fixture, tmp_path):
    # The file is kept written while empty. A file before it fails to parse on every other load.
    # Those loads still read and hold back this file, so they do not start its bound over.
    clock = FakeClock()
    first = os.path.join(str(tmp_path), 'first.json')
    write_file(first, '{"flagValues": {"flag3": true}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([first, f.path], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    require_held_back(f)
    for i in range(1, 10):
        clock.advance(0.5 * HOUR)
        change_modification_time(f.path)
        write_file(first, '{"flagValues"' if i % 2 == 1 else '{"flagValues": {"flag3": true}}')
        assert load(f) is None

    clock.advance(0.5 * HOUR)
    change_modification_time(f.path)
    write_file(first, '{"flagValues": {"flag3": true}}')
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag3']


@yaml_required
def test_reloader_bound_of_a_file_after_a_failing_file_starts_over_once_it_is_read_again(make_fixture, tmp_path):
    # The file is held back, then a file configured before it breaks while this one gets its
    # content back. The failing loads still read this file, and they hold nothing back, so its
    # bound starts over. Much later the other file is fixed while this one is rewritten in place,
    # and the truncation is held back.
    clock = FakeClock()
    first = os.path.join(str(tmp_path), 'first.json')
    write_file(first, '{"flagValues": {"flag3": true}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([first, f.path], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    require_held_back(f)
    write_file(first, '{"flagValues"')
    write_file(f.path, YAML_FLAG2)
    assert load(f) is None
    for _ in range(6):
        clock.advance(HOUR)
        assert load(f) is None
    assert isinstance(f.errored.get_nowait(), FileReadError)
    assert f.errored.empty()

    write_file(first, '{"flagValues": {"flag3": true}}')
    truncate(f.path)
    require_held_back(f)


@yaml_required
def test_reloader_reads_a_file_after_one_that_cannot_be_read(make_fixture, tmp_path):
    # A file configured before this one goes missing, and missing files are not skipped, so it
    # cannot be read. The failing loads still read this file, which got its content back, and
    # they hold nothing back, so its bound starts over.
    clock = FakeClock()
    first = os.path.join(str(tmp_path), 'first.json')
    write_file(first, '{"flagValues": {"flag3": true}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([first, f.path], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    require_held_back(f)
    os.remove(first)
    write_file(f.path, YAML_FLAG2)
    assert load(f) is None
    assert f.errored.get_nowait().path == first
    clock.advance(6 * HOUR)
    assert load(f) is None

    write_file(first, '{"flagValues": {"flag3": true}}')
    truncate(f.path)
    require_held_back(f)


@yaml_required
def test_reloader_starts_the_bound_of_a_file_that_cannot_be_read_over(make_fixture, monkeypatch):
    # The file is held back, then cannot be read for a long time, then is readable again and
    # empty after a new write. The failing loads hold nothing back and tried to read the file,
    # so its bound started over, and the new emptying is held back.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)

    original = filedata._read_file
    unreadable = [True]

    def read_file(path: str, skip_missing_paths: bool):
        if path == f.path and unreadable[0]:
            raise FileReadError(path, "unable to read file: permission denied")
        return original(path, skip_missing_paths)

    monkeypatch.setattr(filedata, '_read_file', read_file)
    for _ in range(6):
        assert load(f) is None
        clock.advance(HOUR)
    assert isinstance(f.errored.get_nowait(), FileReadError)

    unreadable[0] = False
    change_modification_time(f.path)
    require_held_back(f)


@yaml_required
def test_reloader_keeps_the_delay_of_a_held_back_file_through_a_failed_read(make_fixture, monkeypatch):
    # A read that fails says nothing about the content, so the file's delay goes on. Once it can
    # be read again, still empty and unmodified, it has been empty for the whole delay.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)

    original = filedata._read_file
    unreadable = [True]

    def read_file(path: str, skip_missing_paths: bool):
        if path == f.path and unreadable[0]:
            raise FileReadError(path, "unable to read file: permission denied")
        return original(path, skip_missing_paths)

    monkeypatch.setattr(filedata, '_read_file', read_file)
    clock.advance(0.5 * HOUR)
    assert load(f) is None
    assert isinstance(f.errored.get_nowait(), FileReadError)

    unreadable[0] = False
    clock.advance(0.5 * HOUR)
    result = load(f)
    assert result is not None and result.flags == {}


def test_reloader_holds_back_a_json_file_that_holds_only_a_byte_order_mark(make_fixture, caplog):
    # A byte order mark is not content, so a JSON file in effect that holds only one, as a read
    # can see of a file being written, is held back quietly.
    caplog.set_level(logging.DEBUG)
    f, clock = make_held_fixture(make_fixture, '{"flagValues": {"flag1": true}}', name='data.json')
    with open(f.path, 'wb') as out:
        out.write(b'\xef\xbb\xbf')
    require_held_back(f)
    assert deferrals_logged(caplog) == 1


def test_reloader_reports_a_json_file_that_is_not_an_object(make_fixture):
    f, clock = make_held_fixture(make_fixture, '{"flagValues": {"flag1": true}}', name='data.json')
    write_file(f.path, 'null')
    assert load(f) is None
    err = f.errored.get_nowait()
    assert isinstance(err, FileReadError)
    assert 'must be an object' in str(err)
    assert 'as JSON, the format of its name' in str(err)


def test_reloader_reports_the_first_failure_when_an_empty_file_also_fails(make_fixture, tmp_path):
    # The first file cannot be parsed, and the second is empty, which as JSON fails too. The load
    # reports the first failure, and logs the other at Debug level.
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues"')
    write_file(second, '')
    f = make_fixture('{}')
    f.reloader = Reloader([first, second], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    err = f.require_errored()
    assert isinstance(err, FileReadError)
    assert err.path == first


def test_reloader_logs_the_failures_after_the_first(make_fixture, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues"')
    write_file(second, '{"flags"')
    f = make_fixture('{}')
    f.reloader = Reloader([first, second], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    assert f.require_errored().path == first
    assert any(r.levelno == logging.DEBUG and second in r.getMessage() for r in caplog.records)


def test_reloader_failing_load_reports_the_first_failure(make_fixture, tmp_path):
    # Every file is read even after one fails, and the load reports the first failure.
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues"')
    write_file(second, '{"flags"')
    f = make_fixture('{}')
    f.reloader = Reloader([first, second], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put)
    f.reloader.reload_now()
    err = f.require_errored()
    assert isinstance(err, FileReadError)
    assert err.path == first


@yaml_required
def test_reloader_applies_the_same_bytes_moved_to_a_file_of_another_format(make_fixture, tmp_path):
    # The format comes from the extension, so the same bytes moved from x.yaml to x.json parse
    # differently: YAML reads 1e3 as a string, JSON as a number. The load is not skipped as
    # unchanged.
    yaml_path = os.path.join(str(tmp_path), 'x.yaml')
    json_path = os.path.join(str(tmp_path), 'x.json')
    content = '{"flagValues": {"flag1": 1e3}}'
    write_file(yaml_path, content)
    f = make_fixture('{}')
    f.reloader = Reloader([yaml_path, json_path], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, skip_missing_paths=True, skip_unchanged=True)
    f.reloader.reload_now()
    assert f.require_applied().flags['flag1'].variations == ['1e3']

    os.replace(yaml_path, json_path)
    result = load(f)
    assert result is not None and result.flags['flag1'].variations == [1000.0]


@yaml_required
def test_reloader_load_that_holds_a_file_back_and_then_fails_does_not_start_bounds_over(make_fixture, tmp_path):
    # Two files are rewritten in turn, and a third fails to parse on every other load, after one of
    # the two has been held back. Those loads hold a file back, so they do not start the bounds
    # over, and the files are applied in the end.
    clock = FakeClock()
    other = os.path.join(str(tmp_path), 'other.yaml')
    last = os.path.join(str(tmp_path), 'last.json')
    write_file(other, YAML_FLAG2)
    write_file(last, '{"flagValues": {"flag3": true}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, other, last], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    require_held_back(f)
    applied = False
    for i in range(1, 13):
        clock.advance(0.5 * HOUR)
        if i % 2 == 1:
            write_file(f.path, YAML_FLAG1)
            truncate(other)
            write_file(last, '{"flagValues"')
        else:
            write_file(other, YAML_FLAG2)
            truncate(f.path)
            write_file(last, '{"flagValues": {"flag3": true}}')
        if load(f) is not None:
            applied = True
            break
    assert applied, "the files were held back without end"


@yaml_required
def test_reloader_file_found_missing_starts_a_new_delay_when_it_is_created_empty_again(make_fixture, tmp_path):
    # The file is emptied and held back, then deleted, then created empty again with the
    # modification time of its first emptying, while another file fails to parse, so nothing is
    # applied meanwhile. Being found missing ended its empty state, so it starts a new delay.
    clock = FakeClock()
    broken = os.path.join(str(tmp_path), 'other.json')
    write_file(broken, '{"flagValues": {"flag3": true}}')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, broken], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, skip_missing_paths=True, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    f.require_applied()

    write_file(broken, '{bad')
    truncate(f.path)
    first_emptying = os.stat(f.path)
    assert load(f) is None
    os.remove(f.path)
    assert load(f) is None

    clock.advance(70 / 60 * HOUR)
    write_file(f.path, '')
    os.utime(f.path, ns=(first_emptying.st_atime_ns, first_emptying.st_mtime_ns))
    write_file(broken, '{"flagValues": {"flag3": true}}')
    while not f.errored.empty():
        f.errored.get_nowait()
    require_held_back(f)


@yaml_required
def test_reloader_hold_asks_for_the_retry_after_the_delay_and_a_tenth(make_fixture):
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(1.1 * HOUR)


@yaml_required
def test_reloader_later_hold_asks_for_the_rest_of_the_delay(make_fixture):
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    clock.advance(0.4 * HOUR)
    require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(0.6 * HOUR + 0.1 * HOUR)


@yaml_required
def test_reloader_hold_asks_for_the_retry_when_the_bound_is_over(make_fixture):
    # The file was written again, so its delay would end one retry delay from now, but its bound
    # ends sooner.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    for _ in range(9):
        clock.advance(0.5 * HOUR)
        change_modification_time(f.path)
        require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(0.5 * HOUR + 0.1 * HOUR)


@yaml_required
@pytest.mark.parametrize('due_first', ['first', 'last'])
def test_reloader_hold_asks_for_the_retry_when_the_first_held_file_is_due(make_fixture, tmp_path, due_first):
    f, clock, other = make_two_file_fixture(make_fixture, tmp_path, YAML_FLAG1, YAML_FLAG2)
    earlier, later = (f.path, other) if due_first == 'first' else (other, f.path)
    truncate(earlier)
    require_held_back(f)
    clock.advance(0.5 * HOUR)
    truncate(later)
    require_held_back(f)
    assert f.reloader._last_requested_retry_delay == pytest.approx(0.5 * HOUR + 0.1 * HOUR)


@yaml_required
def test_reloader_hold_after_a_change_signal_asks_for_the_same_retry(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)

    def wait_for_requested_delay(expected: float) -> None:
        deadline = time.monotonic() + TEST_TIMEOUT
        while f.reloader._last_requested_retry_delay != pytest.approx(expected):
            assert time.monotonic() < deadline, "asked for %r" % f.reloader._last_requested_retry_delay
            time.sleep(0.01)

    truncate(f.path)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)
    wait_for_requested_delay(1.1 * HOUR)
    clock.advance(0.4 * HOUR)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 2)
    wait_for_requested_delay(0.7 * HOUR)


def test_reloader_failure_asks_for_the_retry_delay(make_fixture):
    f = make_fixture('{"flagValues"', retry_delay=HOUR, clock=FakeClock())
    f.reloader.reload_now()
    f.require_errored()
    assert f.reloader._last_requested_retry_delay == HOUR


@yaml_required
def test_reloader_holds_back_a_file_that_is_emptied_again_after_its_content_was_applied(make_fixture):
    # A load that holds no file back ends the run, so a later rewrite is held back on its own.
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1)
    truncate(f.path)
    require_held_back(f)
    write_file(f.path, YAML_FLAG2)
    clock.advance(0.5 * HOUR)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']

    clock.advance(10 * HOUR)
    truncate(f.path)
    require_held_back(f)
    clock.advance(HOUR)
    result = load(f)
    assert result is not None and result.flags == {}


def test_reloader_holds_back_a_json_file_that_was_empty_for_long_when_it_is_rewritten(make_fixture, caplog):
    # A JSON file that stays empty fails to load once the delay is over, and keeps failing. The
    # failures end the run of holds, so when the file is rewritten in place much later, the new
    # truncation is held back like any other, without another report.
    caplog.set_level(logging.DEBUG)
    f, clock = make_held_fixture(make_fixture, '{"flagValues": {"flag1": true}}', name='data.json')
    truncate(f.path)
    require_held_back(f)

    clock.advance(HOUR)
    assert load(f) is None
    assert 'no content' in str(f.errored.get_nowait())
    clock.advance(HOUR)
    assert load(f) is None
    assert f.errored.empty()

    clock.advance(10 * HOUR)
    held_before = deferrals_logged(caplog)
    truncate(f.path)
    change_modification_time(f.path)
    require_held_back(f)
    assert deferrals_logged(caplog) == held_before + 1

    write_file(f.path, '{"flagValues": {"flag1": false}}')
    result = load(f)
    assert result is not None and result.flags['flag1'].variations == [False]


@yaml_required
def test_reloader_does_not_hold_back_a_file_that_is_empty_from_the_start(make_fixture, tmp_path, caplog):
    # An empty YAML file next to the data, such as a placeholder for later entries, has never had
    # content. It does not hold back the changes of the other file.
    caplog.set_level(logging.DEBUG)
    clock = FakeClock()
    placeholder = os.path.join(str(tmp_path), 'placeholder.yaml')
    write_file(placeholder, '')
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader = Reloader([f.path, placeholder], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, retry_delay=HOUR, clock=clock)
    f.reloader.reload_now()
    assert flag_keys(f.require_applied()) == ['flag1']

    write_file(f.path, '{"flagValues": {"flag1": false}}')
    result = load(f)
    assert result is not None and result.flags['flag1'].variations == [False]
    assert deferrals_logged(caplog) == 0


@yaml_required
def test_reloader_applies_a_file_that_is_deleted_and_then_created_empty_at_once(make_fixture):
    f, clock = make_held_fixture(make_fixture, YAML_FLAG1, skip_missing_paths=True)

    # Deleting a file that had content removes its data at once.
    os.remove(f.path)
    result = load(f)
    assert result is not None and result.flags == {}

    # A missing file contributed no content, so the empty file that replaces it is applied at once.
    write_file(f.path, '')
    result = load(f)
    assert result is not None and result.flags == {}
    assert result.files == [FileSummary(path=f.path, present=True)]


@yaml_required
def test_reloader_does_not_hold_back_a_file_whose_content_moved_away(make_fixture, tmp_path):
    # The content moves from one file to the other. The file that no longer exists contributes no
    # content, so when it is created empty, that is applied at once.
    clock = FakeClock()
    other = os.path.join(str(tmp_path), 'other.yaml')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, other], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, skip_missing_paths=True, retry_delay=HOUR, skip_unchanged=True, clock=clock)
    f.reloader.reload_now()
    assert flag_keys(f.require_applied()) == ['flag1']

    write_file(other, YAML_FLAG1)
    os.remove(f.path)
    result = load(f)
    assert result is not None and result.files == [FileSummary(path=f.path), FileSummary(path=other, present=True, flags=1)]

    write_file(f.path, '')
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag1']
    assert result.files == [FileSummary(path=f.path, present=True), FileSummary(path=other, present=True, flags=1)]


@yaml_required
def test_reloader_holds_back_a_file_whose_content_was_moved_to_it_from_another(make_fixture, tmp_path):
    # The file is renamed to the other configured path (mv data.yaml other.yaml). The other
    # path's content is in effect now, so an in-place rewrite of it is held back.
    clock = FakeClock()
    other = os.path.join(str(tmp_path), 'other.yaml')
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader = Reloader([f.path, other], DuplicateKeysHandling.FAIL, apply=f.applied.put, on_error=f.errored.put, skip_missing_paths=True, retry_delay=HOUR, skip_unchanged=True, clock=clock)
    f.reloader.reload_now()
    assert flag_keys(f.require_applied()) == ['flag1']

    os.replace(f.path, other)
    result = load(f)
    assert result is not None and result.files == [FileSummary(path=f.path), FileSummary(path=other, present=True, flags=1)]

    truncate(other)
    require_held_back(f)
    write_file(other, YAML_FLAG2)
    result = load(f)
    assert result is not None and flag_keys(result) == ['flag2']


@yaml_required
def test_reloader_applies_an_emptied_file_at_once_when_retries_are_off(make_fixture, caplog):
    # Nothing would load the file later, so the empty file is parsed as it is.
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml')
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    result = load(f)
    assert result is not None and result.flags == {}
    assert deferrals_logged(caplog) == 0


def test_reloader_reports_an_emptied_json_file_at_once_when_retries_are_off(make_fixture):
    f = make_fixture('{"flagValues": {"flag1": true}}')
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    assert load(f) is None
    assert 'no content' in str(f.errored.get_nowait())


def test_reloader_hold_is_not_a_failure(make_fixture, caplog):
    # A rewrite in place that writes the same content again changes nothing. The load that finds
    # the file empty reports nothing, and is not a failure that a later load recovers from, so
    # the unchanged content is not applied again.
    caplog.set_level(logging.DEBUG)
    content = '{"flagValues": {"flag1": true}}'
    f, clock = make_held_fixture(make_fixture, content, name='data.json', skip_unchanged=True)
    truncate(f.path)
    require_held_back(f)
    write_file(f.path, content)
    assert load(f) is None
    assert f.errored.empty()
    assert not any(r.levelno >= logging.WARNING for r in caplog.records), [r.getMessage() for r in caplog.records]


@yaml_required
def test_reloader_initial_load_reads_an_empty_yaml_file_as_no_entries(make_fixture):
    f = make_fixture('', name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    result = f.require_applied()
    assert result.flags == {}
    assert result.files == [FileSummary(path=f.path, present=True)]


def test_reloader_initial_load_fails_on_an_empty_json_file(make_fixture):
    f = make_fixture('', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    assert 'no content' in str(f.require_errored())
    f.require_quiet(0.1)


# Real-clock tests: the retries that holds arm do fire.

@yaml_required
def test_reloader_holds_back_an_emptied_file_until_its_write_triggers_a_load(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY, skip_unchanged=True)
    f.reloader.reload_now()
    assert flag_keys(f.require_applied()) == ['flag1']

    truncate(f.path)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)
    f.require_quiet(0.1)

    # The write triggers a load that applies the new content. Nothing was applied in between.
    write_file(f.path, YAML_FLAG2)
    f.reloader.trigger()
    assert flag_keys(f.require_applied(BEFORE_THE_RETRY)) == ['flag2']
    f.require_quiet(0.1)


@yaml_required
def test_reloader_applies_an_emptied_yaml_file_that_stays_empty_at_the_retry(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    started = time.monotonic()
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)

    # The retry that the hold armed comes when the file has stayed empty and unmodified for the
    # retry delay, so that first retry applies it as a document with no entries.
    assert f.require_applied().flags == {}
    elapsed = time.monotonic() - started
    assert EMPTY_FILE_RETRY_DELAY <= elapsed < 2 * EMPTY_FILE_RETRY_DELAY
    assert deferrals_logged(caplog) == 1


@yaml_required
def test_reloader_a_further_hold_does_not_put_off_the_retry(make_fixture, caplog):
    # A load that holds the file back again arms the retry for the time that is left of the
    # delay, so the file is still applied one retry delay after it was found empty.
    caplog.set_level(logging.DEBUG)
    retry_delay = 3.0
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=retry_delay)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    started = time.monotonic()
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)
    time.sleep(1.5)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 2)

    # A whole retry delay from the second hold would come at 4.5 s.
    assert f.require_applied().flags == {}
    elapsed = time.monotonic() - started
    assert retry_delay <= elapsed < retry_delay + 1.0


def test_reloader_says_why_it_loads_the_files_again(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f = make_fixture('{"flagValues": {"flag1": true}}', retry_delay=0.2)
    f.reloader.reload_now()
    f.require_applied()

    # The retry that a hold arms reads the files again because a file was found empty. It finds
    # the file still empty, which fails as JSON, and the retry after that failure says so.
    truncate(f.path)
    f.reloader.trigger()
    for expected in ('Reading flag data again after a file was found empty', 'Retrying flag data load after earlier failure'):
        deadline = time.monotonic() + TEST_TIMEOUT
        while not any(r.getMessage() == expected for r in caplog.records):
            assert time.monotonic() < deadline, "no log line %r" % expected
            time.sleep(0.01)


def test_reloader_reports_an_emptied_json_file_only_at_the_retry_and_keeps_the_data(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f = make_fixture('{"flagValues": {"flag1": true}}', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    f.require_applied()

    # The load that the trigger starts holds the file back instead of reporting it. The retry
    # finds it still empty and unmodified, and an empty file is not a JSON document.
    truncate(f.path)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)
    f.require_quiet(0.1)
    err = f.require_errored()
    assert isinstance(err, FileReadError)
    assert 'no content' in str(err)
    f.require_quiet(0.1)


@yaml_required
def test_reloader_holds_back_a_second_rewrite_that_the_retry_of_the_first_finds_empty(make_fixture, caplog):
    # The first rewrite is held back. Its content is written and the file is truncated again by
    # a second rewrite before any load reads that content, as when a polled file is rewritten
    # about a second apart and the poll sees only the second truncation. The retry that the
    # first rewrite armed finds the file empty again, and must not apply it.
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    f.require_applied()

    truncate(f.path)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)

    # No trigger: only the retry reads the file.
    write_file(f.path, YAML_FLAG2)
    truncate(f.path)
    change_modification_time(f.path)
    wait_for_deferrals(caplog, 2)
    f.require_quiet(0.2)

    write_file(f.path, 'flagValues:\n  flag3: true\n')
    f.reloader.trigger()
    assert flag_keys(f.require_applied(BEFORE_THE_RETRY)) == ['flag3']


@yaml_required
def test_reloader_retry_that_finds_a_file_emptied_holds_it_back(make_fixture, caplog):
    # A retry left over from a failure can run while the file is being rewritten in place. It
    # holds the file back too, rather than applying it empty.
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY)
    f.reloader.reload_now()
    f.require_applied()
    write_file(f.path, 'flagValues: [')
    f.reloader.trigger()
    f.require_errored()

    # No trigger: only the retry reads the file.
    truncate(f.path)
    wait_for_deferrals(caplog, 1)
    write_file(f.path, YAML_FLAG2)
    assert flag_keys(f.require_applied()) == ['flag2']
    f.require_quiet(0.1)


@yaml_required
def test_reloader_retry_left_over_from_a_hold_applies_nothing_after_a_load_on_demand(make_fixture, caplog):
    caplog.set_level(logging.DEBUG)
    f = make_fixture(YAML_FLAG1, name='data.yaml', retry_delay=EMPTY_FILE_RETRY_DELAY, skip_unchanged=True)
    f.reloader.reload_now()
    f.require_applied()
    truncate(f.path)
    f.reloader.trigger()
    wait_for_deferrals(caplog, 1)

    # A load on demand reads the rewritten file before the retry does.
    write_file(f.path, YAML_FLAG2)
    f.reloader.reload_now()
    assert flag_keys(f.require_applied()) == ['flag2']

    # The retry still runs. It finds the content that was just applied, so it applies and
    # reports nothing.
    f.require_quiet(EMPTY_FILE_RETRY_DELAY + 0.5)


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

    def drain(self, settle: float) -> None:
        """Discards the change callbacks that arrive within the settle window.

        A plain rewrite is not atomic: the truncation, the write, and the modification-time
        update are separate steps, and a poll that lands between two of them legitimately
        reports an extra change. A test that wants to assert silence waits for the write to
        be fully observed first.
        """
        deadline = time.monotonic() + settle
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                self.changes.get(timeout=remaining)
            except Empty:
                return


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


def test_poller_stops_firing_when_file_is_stable(tmp_path, make_poller):
    # The poller updates its baseline after a change, so it goes quiet once the file is stable.
    # It does not promise one callback per logical change: a rewrite that it observes in more
    # than one step is reported in more than one step, and the reloader absorbs the excess.
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    p = make_poller([path])
    write_file(path, 'bb')
    p.require_change()
    p.drain(POLL_INTERVAL * 10)
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
    # Older watchdog versions report no event for a read-only close.
    closed_no_write = getattr(watchdog.events, 'FileClosedNoWriteEvent', None)
    if closed_no_write is not None:
        w.watcher._handle_event(closed_no_write(real_path))
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
def test_watcher_watches_a_directory_again_after_it_is_deleted_and_recreated(tmp_path, make_watcher, monkeypatch):
    monkeypatch.setattr(filedata, '_WATCH_RETRY_INTERVAL', SHORT_DELAY)
    directory = os.path.join(str(tmp_path), 'later')
    path = os.path.join(directory, 'data.json')
    os.mkdir(directory)
    write_file(path, 'a')
    w = make_watcher([path])

    # The file and then its directory are removed.
    os.remove(path)
    os.rmdir(directory)
    w.drain()

    # The directory comes back with the file. The retry that watches it again signals a change.
    os.mkdir(directory)
    write_file(path, 'bb')
    w.require_change()
    w.drain()

    # Notifications from the recreated directory are delivered.
    write_file(path, 'ccc')
    w.require_change()


@watchdog_required
def test_watcher_retries_when_the_observer_cannot_start(tmp_path, make_watcher, monkeypatch, caplog):
    import watchdog.observers

    monkeypatch.setattr(filedata, '_WATCH_RETRY_INTERVAL', SHORT_DELAY)
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')

    # The observer cannot start while the watcher is constructed.
    with caplog.at_level(logging.ERROR):
        with mock.patch.object(watchdog.observers.Observer, 'start', side_effect=RuntimeError("cannot start thread")):
            w = make_watcher([path])
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) > 0

    # Once the observer can start, the retry sets up the watch and signals a change, and later
    # edits are detected.
    w.require_change()
    w.drain()
    write_file(path, 'bb')
    w.require_change()


@watchdog_required
def test_watcher_close_succeeds_when_the_observer_never_started(tmp_path):
    import watchdog.observers

    path = os.path.join(str(tmp_path), 'data.json')
    with mock.patch.object(watchdog.observers.Observer, 'start', side_effect=RuntimeError("cannot start thread")):
        watcher = Watcher([path], lambda: None)
        watcher.close()


@watchdog_required
def test_watcher_close_stops_notifications(tmp_path, make_watcher):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'a')
    w = make_watcher([path])
    w.watcher.close()
    write_file(path, 'bb')
    w.require_no_change()
