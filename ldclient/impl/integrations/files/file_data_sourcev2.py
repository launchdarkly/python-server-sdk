import json
import os
import threading
import traceback
from queue import Empty, Queue
from typing import Generator

from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import _Fail, _Success, current_time_millis, log
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    Update
)

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
    if not ("version" in item):
        item["version"] = 1


class _FileDataSourceV2:
    """
    Internal implementation of both Initializer and Synchronizer protocols for file-based data.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.

    This component reads feature flag and segment data from local files and provides them
    via the FDv2 protocol interfaces. Each instance implements both Initializer and Synchronizer
    protocols:
    - As an Initializer: reads files once and returns initial data
    - As a Synchronizer: watches for file changes and yields updates

    The files use the same format as the v1 file data source, supporting flags, flagValues,
    and segments in JSON or YAML format.
    """

    def __init__(self, paths, poll_interval=1, force_polling=False):
        """
        Initialize the file data source.

        :param paths: list of file paths to load (or a single path string)
        :param poll_interval: seconds between polling checks when watching files (default: 1)
        :param force_polling: force polling even if watchdog is available (default: False)
        """
        self._paths = paths if isinstance(paths, list) else [paths]
        self._poll_interval = poll_interval
        self._force_polling = force_polling
        self._closed = False
        self._update_queue = Queue()
        self._lock = threading.Lock()
        self._auto_updater = None

    @property
    def name(self) -> str:
        """Return the name of this data source."""
        return "FileDataV2"

    def fetch(self, ss: SelectorStore) -> BasisResult:
        """
        Implementation of the Initializer.fetch method.

        Reads all configured files once and returns their contents as a Basis.

        :param ss: SelectorStore (not used, as we don't have selectors for file data)
        :return: BasisResult containing the file data or an error
        """
        try:
            with self._lock:
                if self._closed:
                    return _Fail("FileDataV2 source has been closed")

                # Load all files and build changeset
                result = self._load_all_to_changeset()
                if isinstance(result, _Fail):
                    return result

                change_set = result.value

                basis = Basis(change_set=change_set, persist=False, environment_id=None)

                return _Success(basis)

        except Exception as e:
            log.error("Error fetching file data: %s" % repr(e))
            traceback.print_exc()
            return _Fail(f"Error fetching file data: {str(e)}")

    def sync(self, ss: SelectorStore) -> Generator[Update, None, None]:
        """
        Implementation of the Synchronizer.sync method.

        Yields initial data from files, then continues to watch for file changes
        and yield updates when files are modified.

        :param ss: SelectorStore (not used, as we don't have selectors for file data)
        :return: Generator yielding Update objects
        """
        # First yield initial data
        initial_result = self.fetch(ss)
        if isinstance(initial_result, _Fail):
            yield Update(
                state=DataSourceState.OFF,
                error=DataSourceErrorInfo(
                    kind=DataSourceErrorKind.INVALID_DATA,
                    status_code=0,
                    time=current_time_millis(),
                    message=initial_result.error,
                ),
            )
            return

        # Yield the initial successful state
        yield Update(
            state=DataSourceState.VALID, change_set=initial_result.value.change_set
        )

        # Start watching for file changes
        with self._lock:
            if not self._closed:
                self._auto_updater = self._start_auto_updater()

        # Continue yielding updates as they arrive
        while not self._closed:
            try:
                # Wait for updates with a timeout to allow checking closed status
                try:
                    update = self._update_queue.get(timeout=1.0)
                except Empty:
                    continue

                if update is None:  # Sentinel value for shutdown
                    break

                yield update

            except Exception as e:
                log.error("Error in file data synchronizer: %s" % repr(e))
                traceback.print_exc()
                yield Update(
                    state=DataSourceState.OFF,
                    error=DataSourceErrorInfo(
                        kind=DataSourceErrorKind.UNKNOWN,
                        status_code=0,
                        time=current_time_millis(),
                        message=f"Error in file data synchronizer: {str(e)}",
                    ),
                )
                break

    def stop(self):
        """Stop the data source and clean up resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

            auto_updater = self._auto_updater
            self._auto_updater = None

        if auto_updater:
            auto_updater.stop()

        # Signal shutdown to sync generator
        self._update_queue.put(None)

    def _load_all_to_changeset(self):
        """
        Load all files and build a changeset.

        :return: _Result containing ChangeSet or error string
        """
        flags_dict = {}
        segments_dict = {}

        for path in self._paths:
            try:
                self._load_file(path, flags_dict, segments_dict)
            except Exception as e:
                log.error('Unable to load flag data from "%s": %s' % (path, repr(e)))
                traceback.print_exc()
                return _Fail(f'Unable to load flag data from "{path}": {str(e)}')

        # Build a full transfer changeset
        builder = ChangeSetBuilder()
        builder.start(IntentCode.TRANSFER_FULL)

        # Add all flags to the changeset
        for key, flag_data in flags_dict.items():
            builder.add_put(
                ObjectKind.FLAG, key, flag_data.get("version", 1), flag_data
            )

        # Add all segments to the changeset
        for key, segment_data in segments_dict.items():
            builder.add_put(
                ObjectKind.SEGMENT, key, segment_data.get("version", 1), segment_data
            )

        # Use no_selector since we don't have versioning information from files
        change_set = builder.finish(Selector.no_selector())

        return _Success(change_set)

    def _load_file(self, path, flags_dict, segments_dict):
        """
        Load a single file and add its contents to the provided dictionaries.

        :param path: path to the file
        :param flags_dict: dictionary to add flags to
        :param segments_dict: dictionary to add segments to
        """
        content = None
        with open(path, "r") as f:
            content = f.read()
        parsed = self._parse_content(content)

        for key, flag in parsed.get("flags", {}).items():
            _sanitize_json_item(flag)
            self._add_item(flags_dict, "flags", flag)

        for key, value in parsed.get("flagValues", {}).items():
            self._add_item(flags_dict, "flags", self._make_flag_with_value(key, value))

        for key, segment in parsed.get("segments", {}).items():
            _sanitize_json_item(segment)
            self._add_item(segments_dict, "segments", segment)

    def _parse_content(self, content):
        """
        Parse file content as JSON or YAML.

        :param content: file content string
        :return: parsed dictionary
        """
        if have_yaml:
            return yaml.safe_load(content)  # pyyaml correctly parses JSON too
        return json.loads(content)

    def _add_item(self, items_dict, kind_name, item):
        """
        Add an item to a dictionary, checking for duplicates.

        :param items_dict: dictionary to add to
        :param kind_name: name of the kind (for error messages)
        :param item: item to add
        """
        key = item.get("key")
        if items_dict.get(key) is None:
            items_dict[key] = item
        else:
            raise Exception(
                'In %s, key "%s" was used more than once' % (kind_name, key)
            )

    def _make_flag_with_value(self, key, value):
        """
        Create a simple flag configuration from a key-value pair.

        :param key: flag key
        :param value: flag value
        :return: flag dictionary
        """
        return {
            "key": key,
            "version": 1,
            "on": True,
            "fallthrough": {"variation": 0},
            "variations": [value],
        }

    def _start_auto_updater(self):
        """
        Start watching files for changes.

        :return: auto-updater instance
        """
        resolved_paths = []
        for path in self._paths:
            try:
                resolved_paths.append(os.path.realpath(path))
            except Exception:
                log.warning(
                    'Cannot watch for changes to data file "%s" because it is an invalid path'
                    % path
                )

        if have_watchdog and not self._force_polling:
            return _WatchdogAutoUpdaterV2(resolved_paths, self._on_file_change)
        else:
            return _PollingAutoUpdaterV2(
                resolved_paths, self._on_file_change, self._poll_interval
            )

    def _on_file_change(self):
        """
        Callback invoked when files change.

        Reloads all files and queues an update.
        """
        with self._lock:
            if self._closed:
                return

            try:
                # Reload all files
                result = self._load_all_to_changeset()

                if isinstance(result, _Fail):
                    # Queue an error update
                    error_update = Update(
                        state=DataSourceState.INTERRUPTED,
                        error=DataSourceErrorInfo(
                            kind=DataSourceErrorKind.INVALID_DATA,
                            status_code=0,
                            time=current_time_millis(),
                            message=result.error,
                        ),
                    )
                    self._update_queue.put(error_update)
                else:
                    # Queue a successful update
                    update = Update(
                        state=DataSourceState.VALID, change_set=result.value
                    )
                    self._update_queue.put(update)

            except Exception as e:
                log.error("Error processing file change: %s" % repr(e))
                traceback.print_exc()
                error_update = Update(
                    state=DataSourceState.INTERRUPTED,
                    error=DataSourceErrorInfo(
                        kind=DataSourceErrorKind.UNKNOWN,
                        status_code=0,
                        time=current_time_millis(),
                        message=f"Error processing file change: {str(e)}",
                    ),
                )
                self._update_queue.put(error_update)


# Watch for changes to data files using the watchdog package. This uses native OS filesystem notifications
# if available for the current platform.
class _WatchdogAutoUpdaterV2:
    def __init__(self, resolved_paths, on_change_callback):
        watched_files = set(resolved_paths)

        class LDWatchdogHandler(watchdog.events.FileSystemEventHandler):
            def on_any_event(self, event):
                if event.src_path in watched_files:
                    on_change_callback()

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
class _PollingAutoUpdaterV2:
    def __init__(self, resolved_paths, on_change_callback, interval):
        self._paths = resolved_paths
        self._on_change = on_change_callback
        self._file_times = self._check_file_times()
        self._timer = RepeatingTask(
            "ldclient.datasource.filev2.poll", interval, interval, self._poll
        )
        self._timer.start()

    def stop(self):
        self._timer.stop()

    def _poll(self):
        new_times = self._check_file_times()
        changed = False
        for file_path, file_time in self._file_times.items():
            if (
                new_times.get(file_path) is not None
                and new_times.get(file_path) != file_time
            ):
                changed = True
                break
        self._file_times = new_times
        if changed:
            self._on_change()

    def _check_file_times(self):
        ret = {}
        for path in self._paths:
            try:
                ret[path] = os.path.getmtime(path)
            except Exception:
                log.warning(
                    "Failed to get modification time for %s. Setting to None", path
                )
                ret[path] = None
        return ret
