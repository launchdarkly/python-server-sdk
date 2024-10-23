import json
import os
import time
import traceback
from typing import Optional

from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import log
from ldclient.interfaces import (DataSourceErrorInfo, DataSourceErrorKind,
                                 DataSourceState, DataSourceUpdateSink,
                                 UpdateProcessor)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

have_yaml = False
try:
    import yaml

    have_yaml = True
except ImportError:
    pass

have_watchdog = False
try:
    import watchdog
    import watchdog.events
    import watchdog.observers

    have_watchdog = True
except ImportError:
    pass


def _sanitize_json_item(item):
    if not ('version' in item):
        item['version'] = 1


class _FileDataSource(UpdateProcessor):
    def __init__(self, store, data_source_update_sink: Optional[DataSourceUpdateSink], ready, paths, auto_update, poll_interval, force_polling):
        self._store = store
        self._data_source_update_sink = data_source_update_sink
        self._ready = ready
        self._inited = False
        self._paths = paths
        if isinstance(self._paths, str):
            self._paths = [self._paths]
        self._auto_update = auto_update
        self._auto_updater = None
        self._poll_interval = poll_interval
        self._force_polling = force_polling

    def _sink_or_store(self):
        """
        The original implementation of this class relied on the feature store
        directly, which we are trying to move away from. Customers who might have
        instantiated this directly for some reason wouldn't know they have to set
        the config's sink manually, so we have to fall back to the store if the
        sink isn't present.

        The next major release should be able to simplify this structure and
        remove the need for fall back to the data store because the update sink
        should always be present.
        """
        if self._data_source_update_sink is None:
            return self._store

        return self._data_source_update_sink

    def start(self):
        self._load_all()

        if self._auto_update:
            self._auto_updater = self._start_auto_updater()

        # We will signal readiness immediately regardless of whether the file load succeeded or failed -
        # the difference can be detected by checking initialized()
        self._ready.set()

    def stop(self):
        if self._auto_updater:
            self._auto_updater.stop()

    def initialized(self):
        return self._inited

    def _load_all(self):
        all_data = {FEATURES: {}, SEGMENTS: {}}
        for path in self._paths:
            try:
                self._load_file(path, all_data)
            except Exception as e:
                log.error('Unable to load flag data from "%s": %s' % (path, repr(e)))
                traceback.print_exc()
                if self._data_source_update_sink is not None:
                    self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.INVALID_DATA, 0, time.time, str(e)))
                return
        try:
            self._sink_or_store().init(all_data)
            self._inited = True
            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)
        except Exception as e:
            log.error('Unable to store data: %s' % repr(e))
            traceback.print_exc()
            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time, str(e)))

    def _load_file(self, path, all_data):
        content = None
        with open(path, 'r') as f:
            content = f.read()
        parsed = self._parse_content(content)
        for key, flag in parsed.get('flags', {}).items():
            _sanitize_json_item(flag)
            self._add_item(all_data, FEATURES, flag)
        for key, value in parsed.get('flagValues', {}).items():
            self._add_item(all_data, FEATURES, self._make_flag_with_value(key, value))
        for key, segment in parsed.get('segments', {}).items():
            _sanitize_json_item(segment)
            self._add_item(all_data, SEGMENTS, segment)

    def _parse_content(self, content):
        if have_yaml:
            return yaml.safe_load(content)  # pyyaml correctly parses JSON too
        return json.loads(content)

    def _add_item(self, all_data, kind, item):
        items = all_data[kind]
        key = item.get('key')
        if items.get(key) is None:
            items[key] = item
        else:
            raise Exception('In %s, key "%s" was used more than once' % (kind.namespace, key))

    def _make_flag_with_value(self, key, value):
        return {'key': key, 'version': 1, 'on': True, 'fallthrough': {'variation': 0}, 'variations': [value]}

    def _start_auto_updater(self):
        resolved_paths = []
        for path in self._paths:
            try:
                resolved_paths.append(os.path.realpath(path))
            except Exception:
                log.warning('Cannot watch for changes to data file "%s" because it is an invalid path' % path)
        if have_watchdog and not self._force_polling:
            return _FileDataSource.WatchdogAutoUpdater(resolved_paths, self._load_all)
        else:
            return _FileDataSource.PollingAutoUpdater(resolved_paths, self._load_all, self._poll_interval)

    # Watch for changes to data files using the watchdog package. This uses native OS filesystem notifications
    # if available for the current platform.
    class WatchdogAutoUpdater:
        def __init__(self, resolved_paths, reloader):
            watched_files = set(resolved_paths)

            class LDWatchdogHandler(watchdog.events.FileSystemEventHandler):
                def on_any_event(self, event):
                    if event.src_path in watched_files:
                        reloader()

            dir_paths = set()
            for path in resolved_paths:
                dir_paths.add(os.path.dirname(path))

            self._observer = watchdog.observers.Observer()
            handler = LDWatchdogHandler()
            for path in dir_paths:
                self._observer.schedule(handler, path)
            self._observer.start()

        def stop(self):
            self._observer.stop()
            self._observer.join()

    # Watch for changes to data files by polling their modification times. This is used if auto-update is
    # on but the watchdog package is not installed.
    class PollingAutoUpdater:
        def __init__(self, resolved_paths, reloader, interval):
            self._paths = resolved_paths
            self._reloader = reloader
            self._file_times = self._check_file_times()
            self._timer = RepeatingTask("ldclient.datasource.file.poll", interval, interval, self._poll)
            self._timer.start()

        def stop(self):
            self._timer.stop()

        def _poll(self):
            new_times = self._check_file_times()
            changed = False
            for file_path, file_time in self._file_times.items():
                if new_times.get(file_path) is not None and new_times.get(file_path) != file_time:
                    changed = True
                    break
            self._file_times = new_times
            if changed:
                self._reloader()

        def _check_file_times(self):
            ret = {}
            for path in self._paths:
                try:
                    ret[path] = os.path.getmtime(path)
                except Exception:
                    ret[path] = None
            return ret
