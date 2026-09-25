import threading
from queue import Empty, Queue
from typing import Generator, Optional, Union

from ldclient.config import DataSourceBuilder, DataSourceBuilderConfig
from ldclient.impl.integrations.files.filedata import (
    DEFAULT_DEBOUNCE_DELAY,
    DEFAULT_RETRY_DELAY,
    DuplicateKeysHandling,
    FileReadError,
    MergeResult,
    Poller,
    Reloader,
    Watcher,
    abs_file_paths,
    have_watchdog,
    load_files
)
from ldclient.impl.util import _Fail, _Success, current_time_millis, log
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSet,
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


class _FileDataSourceV2:
    """
    Internal implementation of both Initializer and Synchronizer protocols for file-based data.

    This component reads feature flag and segment data from local files and provides them
    via the FDv2 protocol interfaces. Each instance implements both Initializer and Synchronizer
    protocols:
    - As an Initializer: reads files once and returns initial data
    - As a Synchronizer: watches for file changes and yields updates

    The files use the same format as the v1 file data source, supporting flags, flagValues,
    and segments in JSON or YAML format. Every configured file must exist. A key that appears
    in more than one file fails the load.

    As a synchronizer, a load that fails because a file cannot be read or parsed keeps the
    last good data in place, reports an interrupted state, and is retried after a short delay
    and on the next detected change.
    """

    def __init__(self, paths, poll_interval: float = 1, force_polling=False):
        """
        Initialize the file data source.

        :param paths: list of file paths to load (or a single path string)
        :param poll_interval: seconds between polling checks when watching files (default: 1)
        :param force_polling: force polling even if watchdog is available (default: False)
        """
        self._paths = abs_file_paths(paths if isinstance(paths, list) else [paths])
        self._poll_interval = poll_interval
        self._force_polling = force_polling
        self._closed = False
        self._update_queue: Queue[Optional[Update]] = Queue()
        self._lock = threading.Lock()
        self._auto_updater: Optional[Union[Poller, Watcher]] = None
        self._reloader: Optional[Reloader] = None

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
        with self._lock:
            if self._closed:
                return _Fail("FileDataV2 source has been closed")
        try:
            merged = load_files(self._paths, DuplicateKeysHandling.FAIL)
        except Exception as e:
            log.error("Unable to load flag data: %s", e)
            return _Fail("Unable to load flag data: %s" % e)
        return _Success(Basis(change_set=self._make_change_set(merged), persist=False, environment_id=None))

    def sync(self, ss: SelectorStore) -> Generator[Update, None, None]:
        """
        Implementation of the Synchronizer.sync method.

        Loads the files, then continues to watch for file changes and yield updates when
        files are modified. A failed load yields an interrupted state and is retried.

        :param ss: SelectorStore (not used, as we don't have selectors for file data)
        :return: Generator yielding Update objects
        """
        with self._lock:
            if self._closed:
                return
            reloader = Reloader(
                self._paths,
                DuplicateKeysHandling.FAIL,
                apply=self._apply,
                on_error=self._handle_error,
                debounce_delay=DEFAULT_DEBOUNCE_DELAY,
                retry_delay=DEFAULT_RETRY_DELAY,
                skip_unchanged=True,
            )
            self._reloader = reloader

        # The initial load runs synchronously, so its update is queued before any change
        # signal can arrive.
        reloader.reload_now()

        with self._lock:
            if not self._closed:
                self._auto_updater = self._start_auto_updater(reloader)

        while not self._closed:
            try:
                update = self._update_queue.get(timeout=1.0)
            except Empty:
                continue

            if update is None:  # Sentinel value for shutdown
                break

            yield update

    def stop(self):
        """Stop the data source and clean up resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

            auto_updater = self._auto_updater
            self._auto_updater = None
            reloader = self._reloader
            self._reloader = None

        if auto_updater is not None:
            auto_updater.close()
        if reloader is not None:
            reloader.close()

        # Signal shutdown to sync generator
        self._update_queue.put(None)

    def _make_change_set(self, merged: MergeResult) -> ChangeSet:
        """Expresses a merged file data set as a full-transfer changeset."""
        builder = ChangeSetBuilder()
        builder.start(IntentCode.TRANSFER_FULL)
        for key, flag in merged.flags.items():
            builder.add_put(ObjectKind.FLAG, key, flag.version, flag.to_json_dict())
        for key, segment in merged.segments.items():
            builder.add_put(ObjectKind.SEGMENT, key, segment.version, segment.to_json_dict())
        # Use no_selector since we don't have versioning information from files
        return builder.finish(Selector.no_selector())

    def _apply(self, merged: MergeResult) -> None:
        with self._lock:
            if self._closed:
                return
            self._update_queue.put(Update(state=DataSourceState.VALID, change_set=self._make_change_set(merged)))

    def _handle_error(self, error: Exception) -> None:
        kind = DataSourceErrorKind.UNKNOWN if isinstance(error, FileReadError) else DataSourceErrorKind.INVALID_DATA
        with self._lock:
            if self._closed:
                return
            self._update_queue.put(
                Update(
                    state=DataSourceState.INTERRUPTED,
                    error=DataSourceErrorInfo(
                        kind=kind,
                        status_code=0,
                        time=current_time_millis(),
                        message=str(error),
                    ),
                )
            )

    def _start_auto_updater(self, reloader: Reloader) -> Union[Poller, Watcher]:
        """
        Start watching files for changes.

        :return: auto-updater instance
        """
        if have_watchdog and not self._force_polling:
            return Watcher(self._paths, reloader.trigger)
        poller = Poller(self._paths, self._poll_interval, reloader.trigger)
        poller.start()
        return poller


class FileDataSourceV2Builder(DataSourceBuilder):  # pylint: disable=too-few-public-methods
    DEFAULT_POLL_INTERVAL = 1

    def __init__(self, paths: str | list[str]):
        self.__paths = paths
        self.__poll_interval: Optional[float] = None
        self.__force_polling = False

    def poll_interval(self, interval: float) -> 'FileDataSourceV2Builder':
        self.__poll_interval = interval
        return self

    def force_polling(self, force: bool) -> 'FileDataSourceV2Builder':
        self.__force_polling = force
        return self

    def build(self, config: DataSourceBuilderConfig) -> _FileDataSourceV2:  # pylint: disable=unused-argument
        """Builds the FileDataSourceV2 instance."""
        return _FileDataSourceV2(
            self.__paths,
            self.__poll_interval or self.DEFAULT_POLL_INTERVAL,
            self.__force_polling
        )
