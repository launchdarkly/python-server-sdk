"""
Pins the behavior of the two existing file data sources, the FDv1 update processor and the FDv2
initializer and synchronizer. The file-based override source is built on separate code and
behaves differently in several of these respects. These tests keep the existing sources
observably unchanged: the flagValues expansion and its evaluation reason, the version fallback,
the duplicate key and load failure messages, the FDv2 status and error kinds, and the polling
and watching rules.
"""
import logging
import os
import threading
import time
from typing import Any, Callable, List, Optional

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.datasystem import custom
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasource.status import DataSourceUpdateSinkImpl
from ldclient.impl.integrations.files import file_data_sourcev2
from ldclient.impl.integrations.files.file_data_source import _FileDataSource
from ldclient.impl.integrations.files.file_data_sourcev2 import (
    _FileDataSourceV2,
    _PollingAutoUpdaterV2,
    _WatchdogAutoUpdaterV2
)
from ldclient.impl.listeners import Listeners
from ldclient.integrations import Files
from ldclient.interfaces import (
    DataSourceErrorKind,
    DataSourceState,
    ObjectKind,
    Selector
)
from ldclient.testing.mock_components import MockSelectorStore
from ldclient.testing.test_util import SpyListener
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

have_watchdog = file_data_sourcev2.have_watchdog
watchdog_required = pytest.mark.skipif(not have_watchdog, reason="watchdog is not installed")

user = Context.create('user')

DOCUMENT = '''
{
  "flags": {
    "flag1": {
      "key": "flag1",
      "on": true,
      "fallthrough": {"variation": 2},
      "variations": ["fall", "off", "on"]
    },
    "flag-versioned": {
      "key": "flag-versioned",
      "version": 7,
      "on": false,
      "offVariation": 0,
      "variations": ["x"]
    }
  },
  "flagValues": {
    "flag2": "value2"
  },
  "segments": {
    "seg1": {
      "key": "seg1",
      "included": ["user1"]
    }
  }
}
'''

# The expansion of a flagValues entry: an on flag whose fallthrough serves its single variation.
EXPANDED_FLAG2 = {'key': 'flag2', 'version': 1, 'on': True, 'fallthrough': {'variation': 0}, 'variations': ['value2']}


def write_file(path: str, content: str) -> None:
    with open(path, 'w') as f:
        f.write(content)


def make_v1_source(path: str, store: InMemoryFeatureStore, listeners: Optional[Listeners] = None, **kwargs) -> _FileDataSource:
    config = Config('SDK_KEY')
    if listeners is not None:
        config._data_source_update_sink = DataSourceUpdateSinkImpl(store, listeners, Listeners())
    factory = Files.new_data_source(paths=[path], **kwargs)
    assert factory is not None
    source = factory(config, store, threading.Event())
    assert isinstance(source, _FileDataSource)
    return source


def make_v2_source(path: str, **kwargs) -> _FileDataSourceV2:
    source = Files.new_data_source_v2(paths=[path], **kwargs).build(Config('SDK_KEY'))
    assert isinstance(source, _FileDataSourceV2)
    return source


def v2_changes_by_key(change_set) -> dict:
    return {change.key: change for change in change_set.changes}


# ---------------------------------------------------------------------------
# flagValues expansion and its evaluation reason
# ---------------------------------------------------------------------------

def test_v1_expands_flag_values_to_an_on_flag_with_fallthrough(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    store = InMemoryFeatureStore()
    source = make_v1_source(path, store)
    source.start()
    try:
        assert store.get(FEATURES, 'flag2').to_json_dict() == EXPANDED_FLAG2
    finally:
        source.stop()


def test_v1_flag_values_evaluate_with_a_fallthrough_reason(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    config = Config('SDK_KEY', update_processor_class=Files.new_data_source(paths=[path]), send_events=False)
    with LDClient(config) as client:
        detail = client.variation_detail('flag2', user, 'default')
        assert detail.value == 'value2'
        assert detail.variation_index == 0
        assert detail.reason == {'kind': 'FALLTHROUGH'}


def test_v2_expands_flag_values_to_an_on_flag_with_fallthrough(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    source = make_v2_source(path)
    result = source.fetch(MockSelectorStore(Selector.no_selector()))
    changes = v2_changes_by_key(result.value.change_set)
    assert changes['flag2'].object == EXPANDED_FLAG2
    assert changes['flag2'].version == 1


def test_v2_flag_values_evaluate_with_a_fallthrough_reason(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    datasystem = custom().initializers([Files.new_data_source_v2(paths=[path])]).build()
    config = Config('SDK_KEY', datasystem_config=datasystem, send_events=False)
    with LDClient(config) as client:
        detail = client.variation_detail('flag2', user, 'default')
        assert detail.value == 'value2'
        assert detail.variation_index == 0
        assert detail.reason == {'kind': 'FALLTHROUGH'}


# ---------------------------------------------------------------------------
# Version fallback
# ---------------------------------------------------------------------------

def test_v1_stamps_version_1_on_entries_without_one_and_keeps_explicit_versions(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    store = InMemoryFeatureStore()
    source = make_v1_source(path, store)
    source.start()
    try:
        assert store.get(FEATURES, 'flag1').version == 1
        assert store.get(SEGMENTS, 'seg1').version == 1
        assert store.get(FEATURES, 'flag-versioned').version == 7
    finally:
        source.stop()


def test_v2_stamps_version_1_on_entries_without_one_and_keeps_explicit_versions(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, DOCUMENT)
    result = make_v2_source(path).fetch(MockSelectorStore(Selector.no_selector()))
    changes = v2_changes_by_key(result.value.change_set)
    assert changes['flag1'].version == 1
    assert changes['flag1'].object['version'] == 1
    assert changes['seg1'].kind == ObjectKind.SEGMENT
    assert changes['seg1'].version == 1
    assert changes['seg1'].object['version'] == 1
    assert changes['flag-versioned'].version == 7


# ---------------------------------------------------------------------------
# Failure messages, statuses, and error kinds
# ---------------------------------------------------------------------------

def test_v1_logs_the_load_failure_with_the_path_and_reports_invalid_data(tmp_path, caplog):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, '{"flagValues":{')
    store = InMemoryFeatureStore()
    listeners = Listeners()
    spy = SpyListener()
    listeners.add(spy)
    source = make_v1_source(path, store, listeners)
    with caplog.at_level(logging.ERROR):
        source.start()
    try:
        assert store.initialized is False
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any(m.startswith('Unable to load flag data from "%s": ' % path) for m in errors), errors
        assert len(spy.statuses) == 1
        assert spy.statuses[0].error.kind == DataSourceErrorKind.INVALID_DATA
    finally:
        source.stop()


def test_v1_missing_file_fails_the_load_with_the_same_message(tmp_path, caplog):
    path = os.path.join(str(tmp_path), 'missing.json')
    store = InMemoryFeatureStore()
    source = make_v1_source(path, store)
    with caplog.at_level(logging.ERROR):
        source.start()
    try:
        assert source.initialized() is False
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any(m.startswith('Unable to load flag data from "%s": ' % path) for m in errors), errors
    finally:
        source.stop()


def test_v1_duplicate_key_message(tmp_path, caplog):
    first = os.path.join(str(tmp_path), 'first.json')
    second = os.path.join(str(tmp_path), 'second.json')
    write_file(first, '{"flagValues": {"flag1": "a"}}')
    write_file(second, '{"flagValues": {"flag1": "b"}}')
    store = InMemoryFeatureStore()
    config = Config('SDK_KEY')
    source = Files.new_data_source(paths=[first, second])(config, store, threading.Event())
    with caplog.at_level(logging.ERROR):
        source.start()
    try:
        assert store.initialized is False
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any('In features, key "flag1" was used more than once' in m for m in errors), errors
    finally:
        source.stop()


def test_v2_fetch_failure_message_names_the_path(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, '{"flagValues":{')
    result = make_v2_source(path).fetch(MockSelectorStore(Selector.no_selector()))
    assert result.error.startswith('Unable to load flag data from "%s": ' % path)


def test_v2_sync_ends_with_off_when_the_initial_load_fails(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, '{"flagValues":{')
    source = make_v2_source(path, force_polling=True, poll_interval=0.1)
    try:
        updates = list(source.sync(MockSelectorStore(Selector.no_selector())))
        assert len(updates) == 1
        assert updates[0].state == DataSourceState.OFF
        assert updates[0].change_set is None
        assert updates[0].error is not None
        assert updates[0].error.kind == DataSourceErrorKind.INVALID_DATA
        assert updates[0].error.message.startswith('Unable to load flag data from "%s": ' % path)
    finally:
        source.stop()


def test_v2_sync_reports_invalid_data_when_a_file_becomes_malformed(tmp_path):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, '{"flagValues": {"flag1": true}}')
    source = make_v2_source(path, force_polling=True, poll_interval=0.1)
    updates: List[Any] = []
    received = threading.Event()

    def collect():
        for update in source.sync(MockSelectorStore(Selector.no_selector())):
            updates.append(update)
            received.set()
            if len(updates) >= 2:
                break

    thread = threading.Thread(target=collect, daemon=True)
    thread.start()
    try:
        assert received.wait(5)
        assert updates[0].state == DataSourceState.VALID
        received.clear()
        time.sleep(0.2)
        write_file(path, '{"flagValues"')
        assert received.wait(5)
        assert updates[1].state == DataSourceState.INTERRUPTED
        assert updates[1].error.kind == DataSourceErrorKind.INVALID_DATA
        assert updates[1].error.message.startswith('Unable to load flag data from "%s": ' % path)
    finally:
        source.stop()
        thread.join(5)


# ---------------------------------------------------------------------------
# Polling rules: modification time only, a missing file is not a change
# ---------------------------------------------------------------------------

class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1


def set_mtime(path: str, seconds: float) -> None:
    os.utime(path, (seconds, seconds))


@pytest.fixture(params=['v1', 'v2'])
def make_poller(request):
    pollers = []

    def factory(paths: List[str], on_change: Callable[[], None]):
        # The interval is long, so only the direct _poll calls below examine the files.
        poller: Any
        if request.param == 'v1':
            poller = _FileDataSource.PollingAutoUpdater(paths, on_change, 1000)
        else:
            poller = _PollingAutoUpdaterV2(paths, on_change, 1000)
        pollers.append(poller)
        return poller

    yield factory
    for poller in pollers:
        poller.stop()


def test_poller_reloads_when_the_modification_time_changes(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'aaa')
    set_mtime(path, 1000000)
    counter = Counter()
    poller = make_poller([path], counter)
    write_file(path, 'bbb')
    set_mtime(path, 2000000)
    poller._poll()
    assert counter.count == 1


def test_poller_ignores_a_size_change_with_the_same_modification_time(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'aaa')
    set_mtime(path, 1000000)
    counter = Counter()
    poller = make_poller([path], counter)
    write_file(path, 'aaaa')
    set_mtime(path, 1000000)
    poller._poll()
    assert counter.count == 0


def test_poller_ignores_a_file_that_disappears(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    write_file(path, 'aaa')
    counter = Counter()
    poller = make_poller([path], counter)
    os.remove(path)
    poller._poll()
    assert counter.count == 0


def test_poller_reloads_when_a_missing_file_appears(tmp_path, make_poller):
    path = os.path.join(str(tmp_path), 'data.json')
    counter = Counter()
    poller = make_poller([path], counter)
    write_file(path, 'aaa')
    poller._poll()
    assert counter.count == 1


# ---------------------------------------------------------------------------
# Watching rules: any notification on the file's path reloads, a move destination does not
# ---------------------------------------------------------------------------

def handlers_of(observer) -> list:
    return [handler for handlers in observer._handlers.values() for handler in handlers]


@pytest.fixture(params=['v1', 'v2'])
def make_watcher(request):
    watchers = []

    def factory(paths: List[str], on_change: Callable[[], None]):
        watcher: Any
        if request.param == 'v1':
            watcher = _FileDataSource.WatchdogAutoUpdater(paths, on_change)
        else:
            watcher = _WatchdogAutoUpdaterV2(paths, on_change)
        watchers.append(watcher)
        return watcher

    yield factory
    for watcher in watchers:
        watcher.stop()


@watchdog_required
def test_watcher_reloads_on_any_notification_for_the_file_path(tmp_path, make_watcher):
    import watchdog.events

    path = os.path.realpath(os.path.join(str(tmp_path), 'data.json'))
    write_file(path, 'aaa')
    counter = Counter()
    watcher = make_watcher([path], counter)
    handlers = handlers_of(watcher._observer)
    assert len(handlers) == 1
    handler = handlers[0]
    events = [watchdog.events.FileModifiedEvent(path), watchdog.events.FileOpenedEvent(path)]
    # Older watchdog versions report no event for a read-only close.
    closed_no_write = getattr(watchdog.events, 'FileClosedNoWriteEvent', None)
    if closed_no_write is not None:
        events.append(closed_no_write(path))
    for event in events:
        handler.on_any_event(event)
    assert counter.count == len(events)
    handler.on_any_event(watchdog.events.FileModifiedEvent(os.path.join(os.path.dirname(path), 'other.json')))
    assert counter.count == len(events)


@watchdog_required
def test_watcher_ignores_a_move_whose_destination_is_the_file_path(tmp_path, make_watcher):
    import watchdog.events

    path = os.path.realpath(os.path.join(str(tmp_path), 'data.json'))
    temp = os.path.realpath(os.path.join(str(tmp_path), 'data.json.tmp'))
    write_file(path, 'aaa')
    counter = Counter()
    watcher = make_watcher([path], counter)
    handler = handlers_of(watcher._observer)[0]
    handler.on_any_event(watchdog.events.FileMovedEvent(temp, path))
    assert counter.count == 0
    handler.on_any_event(watchdog.events.FileMovedEvent(path, temp))
    assert counter.count == 1
