"""
File reading, parsing, and merging logic shared by the components that load flag and
segment data from local files.

A data file is a JSON or YAML document with optional ``flags``, ``flagValues``, and
``segments`` members. ``flags`` and ``segments`` hold full definitions keyed by key.
``flagValues`` maps a flag key to a single value. It expands into a full flag definition
that returns that value for every context.
"""

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ldclient.impl.model import FeatureFlag, Segment
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import log

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


# A settle window long enough to coalesce the burst of change notifications produced by a
# single file edit, and short enough to stay responsive.
DEFAULT_DEBOUNCE_DELAY = 0.1

# Bounds how long a failed reload can go uncorrected when no further change notification
# arrives, for example when the failure came from reading a file mid-write. Reading a local
# file is cheap, so this can be short.
DEFAULT_RETRY_DELAY = 1.0

# The interval between attempts to watch a directory that could not be watched, for example
# because it does not exist yet.
_WATCH_RETRY_INTERVAL = 1.0

# The watchdog event types that can change a file's content or presence. Opening or reading a
# file also produces events, and a reload reads the files, so those must not count as changes.
_CHANGE_EVENT_TYPES = frozenset(["created", "modified", "moved", "deleted", "closed"])


class DuplicateKeysHandling(str, Enum):
    """
    Determines what happens when the same flag or segment key appears in more than one file.
    """

    FAIL = "fail"
    """A duplicated key causes the load to fail."""

    IGNORE = "ignore"
    """Only the first occurrence of a duplicated key is used, in the order the files were given."""


class FileDataError(Exception):
    """Base class for the errors raised while loading file data."""


class FileReadError(FileDataError):
    """
    Indicates that one of the source files could not be read or parsed. It distinguishes a
    per-file failure from a failure to merge the files' contents.
    """

    def __init__(self, path: str, message: str):
        super().__init__("%s [%s]" % (message, path))
        self.path = path


class DuplicateKeyError(FileDataError):
    """Indicates that the same key appears in more than one file and the handling is FAIL."""


@dataclass
class Document:
    """The parsed form of a single data file."""

    flags: Dict[str, FeatureFlag] = field(default_factory=dict)
    flag_values: Dict[str, Any] = field(default_factory=dict)
    segments: Dict[str, Segment] = field(default_factory=dict)


@dataclass
class DocumentSummary:
    """Counts the entries the merge kept from one document."""

    flags: int = 0
    segments: int = 0


@dataclass
class FileSummary:
    """Describes one configured file after a load."""

    path: str
    present: bool = False
    """False when the file does not exist and missing files are skipped."""
    flags: int = 0
    segments: int = 0


@dataclass
class MergeResult:
    """
    The merged items from one or more documents. The dictionaries preserve document order:
    all of one document's items precede the next document's.
    """

    flags: Dict[str, FeatureFlag] = field(default_factory=dict)
    segments: Dict[str, Segment] = field(default_factory=dict)
    documents: List[DocumentSummary] = field(default_factory=list)
    """For each input document in order, the number of entries the merge kept from it."""
    files: List[FileSummary] = field(default_factory=list)
    """Set by the loader. Describes each configured file in order."""


def abs_file_paths(paths: List[str]) -> List[str]:
    """Converts each of the given paths to an absolute path."""
    return [os.path.abspath(p) for p in paths]


def make_flag_with_value(key: str, value: Any) -> FeatureFlag:
    """
    Expands a flag-key-to-value entry into a full flag definition that returns the given value
    for every context. The flag is off and serves its single variation as the off variation.
    """
    return FeatureFlag({"key": key, "version": 1, "on": False, "offVariation": 0, "variations": [value]})


def read_file(path: str) -> Document:
    """Reads and parses a single data file, which may be in JSON or YAML format."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError as e:
        raise FileReadError(path, "unable to read file: %s" % e)
    try:
        return parse_document(raw)
    except Exception as e:
        raise FileReadError(path, "error parsing file: %s" % e)


def parse_document(raw: bytes) -> Document:
    """
    Parses the raw content of a data file. A document whose first non-blank character is ``{``
    is parsed as JSON. Any other document is parsed as YAML, which requires the ``pyyaml``
    package. An empty document is valid and holds no entries.
    """
    text = raw.decode("utf-8")
    if text.lstrip().startswith("{"):
        parsed = json.loads(text)
    elif have_yaml:
        parsed = yaml.safe_load(text)
    else:
        raise ValueError("the file is not a JSON object and the pyyaml package is not installed, so it cannot be parsed as YAML")
    if parsed is None:
        return Document()
    if not isinstance(parsed, dict):
        raise ValueError("the document must be an object")

    document = Document()
    for key, item in _member_items(parsed, "flags").items():
        document.flags[key] = FeatureFlag(_definition(item, key, "flag"))
    for key, value in _member_items(parsed, "flagValues").items():
        document.flag_values[key] = value
    for key, item in _member_items(parsed, "segments").items():
        document.segments[key] = Segment(_definition(item, key, "segment"))
    return document


def _member_items(parsed: dict, name: str) -> Dict[str, Any]:
    member = parsed.get(name)
    if member is None:
        return {}
    if not isinstance(member, dict):
        raise ValueError('"%s" must be an object' % name)
    for key in member:
        if not isinstance(key, str):
            raise ValueError('"%s" has a key that is not a string: %r' % (name, key))
    return member


def _definition(item: Any, key: str, kind_name: str) -> dict:
    """
    Validates the shape of a flag or segment entry. The entry is stored under its map key. The
    file format allows a definition to omit its own key and version, which the model requires,
    so those two properties are filled in from the map key and a version of 1.
    """
    if not isinstance(item, dict):
        raise ValueError('%s "%s" must be an object' % (kind_name, key))
    if "key" not in item:
        item["key"] = key
    if "version" not in item:
        item["version"] = 1
    return item


def merge(documents: List[Document], duplicate_keys_handling: DuplicateKeysHandling) -> MergeResult:
    """
    Combines the items of the given documents in order, expanding flag-value entries into full
    flag definitions and applying the given duplicate keys handling.
    """
    result = MergeResult()
    seen_flags: Set[str] = set()
    seen_segments: Set[str] = set()

    def insert(items: Dict[str, Any], seen: Set[str], kind_name: str, key: str, item: Any) -> bool:
        if key in seen:
            if duplicate_keys_handling == DuplicateKeysHandling.IGNORE:
                return False
            raise DuplicateKeyError("%s '%s' is specified by multiple files" % (kind_name, key))
        items[key] = item
        seen.add(key)
        return True

    for document in documents:
        summary = DocumentSummary()
        for key, flag in document.flags.items():
            if insert(result.flags, seen_flags, "flag", key, flag):
                summary.flags += 1
        for key, value in document.flag_values.items():
            if insert(result.flags, seen_flags, "flag", key, make_flag_with_value(key, value)):
                summary.flags += 1
        for key, segment in document.segments.items():
            if insert(result.segments, seen_segments, "segment", key, segment):
                summary.segments += 1
        result.documents.append(summary)
    return result


def load_files(paths: List[str], duplicate_keys_handling: DuplicateKeysHandling, skip_missing_paths: bool = False) -> MergeResult:
    """
    Reads, parses, and merges all of the given files in order. Raises :class:`FileReadError`
    when a file cannot be read or parsed, :class:`DuplicateKeyError` when a key is duplicated
    and the handling is FAIL. When ``skip_missing_paths`` is true, a file that does not exist
    contributes no entries instead of failing the load.
    """
    merged, _ = _load_files_hashed(paths, duplicate_keys_handling, skip_missing_paths)
    return merged


def _load_files_hashed(paths: List[str], duplicate_keys_handling: DuplicateKeysHandling, skip_missing_paths: bool) -> Tuple[MergeResult, bytes]:
    """
    The load behind :func:`load_files`. It also returns a digest of the raw file contents, so a
    caller can tell whether a later load read the same bytes. One read feeds both the digest
    and the parse, so the digest can never disagree with the content that was parsed.
    """
    documents: List[Document] = []
    files: List[FileSummary] = []
    hasher = hashlib.sha256()
    for path in paths:
        try:
            with open(path, "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            if skip_missing_paths:
                log.debug("File %s does not exist; it contributes no data", path)
                files.append(FileSummary(path=path))
                continue
            raise FileReadError(path, "unable to read file: the file does not exist")
        except OSError as e:
            raise FileReadError(path, "unable to read file: %s" % e)
        hasher.update(raw)
        hasher.update(b"\0")
        try:
            documents.append(parse_document(raw))
        except Exception as e:
            raise FileReadError(path, "error parsing file: %s" % e)
        files.append(FileSummary(path=path, present=True))

    merged = merge(documents, duplicate_keys_handling)
    # The documents are the present files in order. Copy their counts onto the file summaries.
    next_document = 0
    for summary in files:
        if summary.present:
            summary.flags = merged.documents[next_document].flags
            summary.segments = merged.documents[next_document].segments
            next_document += 1
    merged.files = files
    return merged, hasher.digest()


class Reloader:
    """
    Owns the reload cycle for a set of data files. It serializes reloads, debounces change
    signals, retains the last good result on failure by not calling ``apply``, retries after
    failures, and skips applications that would change nothing.

    The worker thread starts on the first :meth:`reload_now` or :meth:`trigger` call, so a
    reloader that is constructed but never used does not leak a thread.
    """

    def __init__(
        self,
        paths: List[str],
        duplicate_keys_handling: DuplicateKeysHandling,
        apply: Callable[[MergeResult], None],
        on_error: Optional[Callable[[Exception], None]] = None,
        skip_missing_paths: bool = False,
        debounce_delay: float = 0.0,
        retry_delay: float = 0.0,
        skip_unchanged: bool = False,
    ):
        """
        :param paths: the files to load, in order. The order determines which file wins under
          the duplicate keys handling.
        :param duplicate_keys_handling: what to do when the same key appears in more than one file
        :param apply: receives each successfully merged result. Calls are serialized.
        :param on_error: receives each distinct failure. Repeats of an identical failure do not
          call it again until a success re-arms it. Failures are also logged here.
        :param skip_missing_paths: when true, a configured file that does not exist contributes
          no entries. When false, a missing file fails the load like any other read error.
        :param debounce_delay: how long to wait after a trigger for further triggers to settle
          before reloading. Zero reloads on every trigger.
        :param retry_delay: how long to wait after a failed reload before retrying it
          automatically. Zero disables the automatic retry.
        :param skip_unchanged: when true, a load whose raw file contents are identical to the
          last applied contents does not call ``apply``
        """
        self._paths = list(paths)
        self._duplicate_keys_handling = duplicate_keys_handling
        self._apply = apply
        self._on_error = on_error
        self._skip_missing_paths = skip_missing_paths
        self._debounce_delay = debounce_delay
        self._retry_delay = retry_delay
        self._skip_unchanged = skip_unchanged

        # Guards the deadlines, the closed flag, and the worker start.
        self._cond = threading.Condition()
        self._closed = False
        self._started = False
        self._debounce_deadline: Optional[float] = None
        self._retry_deadline: Optional[float] = None

        # Serializes the load work between reload_now and the worker thread.
        self._reload_lock = threading.Lock()
        self._last_good_digest: Optional[bytes] = None
        self._last_error_message: Optional[str] = None

    def reload_now(self) -> None:
        """
        Loads the files synchronously and applies the result or reports the failure. A failure
        arms the same automatic retry as a failed triggered reload.
        """
        self._ensure_started()
        if not self._reload() and self._retry_delay > 0:
            with self._cond:
                if not self._closed and self._retry_deadline is None:
                    self._retry_deadline = time.monotonic() + self._retry_delay
                    self._cond.notify()

    def trigger(self) -> None:
        """
        Signals that the files may have changed. A reload happens after the debounce delay.
        Signals that arrive while a reload is pending extend the settle window.
        """
        self._ensure_started()
        with self._cond:
            if self._closed:
                return
            self._debounce_deadline = time.monotonic() + max(self._debounce_delay, 0.0)
            self._cond.notify()

    def close(self) -> None:
        """
        Stops the reloader. It does not wait for a reload that is already in progress, so such
        a reload may still deliver its result shortly after this returns. A reload that has not
        yet reached its callbacks does not invoke them.
        """
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def _ensure_started(self) -> None:
        with self._cond:
            if self._closed or self._started:
                return
            self._started = True
            thread = threading.Thread(target=self._run, name="ldclient.filedata.reloader", daemon=True)
            thread.start()

    def _run(self) -> None:
        while True:
            is_retry = False
            with self._cond:
                while True:
                    if self._closed:
                        return
                    now = time.monotonic()
                    deadlines = [d for d in (self._debounce_deadline, self._retry_deadline) if d is not None]
                    if len(deadlines) == 0:
                        self._cond.wait()
                        continue
                    next_deadline = min(deadlines)
                    if next_deadline > now:
                        self._cond.wait(next_deadline - now)
                        continue
                    if self._debounce_deadline is not None and self._debounce_deadline <= now:
                        # A triggered reload supersedes a pending retry. It either succeeds, or
                        # it fails and arms a fresh retry below.
                        self._debounce_deadline = None
                        self._retry_deadline = None
                        is_retry = False
                    else:
                        self._retry_deadline = None
                        is_retry = True
                    break

            if is_retry:
                log.debug("Retrying flag data load after earlier failure")
            else:
                log.info("Reloading flag data after detecting a change")
            try:
                ok = self._reload()
            except Exception as e:
                log.exception("Unexpected error while reloading flag data: %s", e)
                ok = True
            if not ok and self._retry_delay > 0:
                with self._cond:
                    if not self._closed:
                        self._retry_deadline = time.monotonic() + self._retry_delay

    def _is_closed(self) -> bool:
        with self._cond:
            return self._closed

    def _reload(self) -> bool:
        """
        Performs one full load of all configured files. Returns whether the load succeeded,
        which decides whether a retry is armed. A skipped no-op application counts as success.
        """
        with self._reload_lock:
            if self._is_closed():
                return True
            try:
                merged, digest = _load_files_hashed(self._paths, self._duplicate_keys_handling, self._skip_missing_paths)
            except Exception as e:
                return self._fail(e)

            # A close may have happened while the files were being read. Deliver nothing then.
            if self._is_closed():
                return True

            # A success right after a failure applies even when the content is unchanged since
            # the last success. The consumer heard about the failure and only an application
            # tells it that things are good again.
            recovering = self._last_error_message is not None
            self._last_error_message = None
            if self._skip_unchanged and not recovering and digest == self._last_good_digest:
                return True
            self._last_good_digest = digest
            self._apply(merged)
            return True

    def _fail(self, err: Exception) -> bool:
        if self._is_closed():
            return True
        # With automatic retries, a persistent failure would repeat the same log entry and the
        # same callback on every attempt. Repeats of an identical failure are logged at debug
        # level and do not call on_error again.
        message = str(err)
        if message == self._last_error_message:
            log.debug("Unable to load flag data: %s", err)
            return False
        self._last_error_message = message
        log.error("Unable to load flag data: %s", err)
        if self._on_error is not None:
            self._on_error(err)
        return False


FileState = Optional[Tuple[int, int]]


def _observe_all(paths: List[str]) -> List[FileState]:
    """
    Observes the state of each file: its modification time and size, or None when it does not
    exist or cannot be examined.
    """
    states: List[FileState] = []
    for path in paths:
        try:
            info = os.stat(path)
            states.append((info.st_mtime_ns, info.st_size))
        except OSError:
            states.append(None)
    return states


class Poller:
    """
    Detects changes to a set of files by examining them on a fixed interval. A change to the
    modification time or the size of any file invokes the callback. A file that appears or
    disappears is also a change. Use it where file system change notifications are not
    available or not reliable.

    Detection is generous. The callback can run for a change that does not alter the
    effective data. Feed it into a :class:`Reloader`, whose debouncing and skip-unchanged
    handling absorb the excess.
    """

    def __init__(self, paths: List[str], interval: float, on_change: Callable[[], None]):
        self._paths = list(paths)
        self._on_change = on_change
        # The files are examined once here, so only later changes invoke the callback.
        self._last = _observe_all(self._paths)
        self._task = RepeatingTask.at_interval("ldclient.filedata.poll", interval, interval, self._poll)

    def start(self) -> None:
        """Starts the polling thread."""
        self._task.start()

    def close(self) -> None:
        """
        Stops the poller. It does not wait for an examination or a callback that is in
        progress, so the callback can run once more shortly after this returns.
        """
        self._task.stop()

    def _poll(self) -> None:
        current = _observe_all(self._paths)
        changed = current != self._last
        self._last = current
        if changed:
            self._on_change()


class Watcher:
    """
    Detects changes to a set of files through file system change notifications, using the
    ``watchdog`` package. The directory of each file is watched, so a file that does not exist
    yet is picked up when it appears. A directory that cannot be watched yet, for example
    because it does not exist, is retried on an interval.

    Notifications for the watched paths invoke the callback. The callback can run several times
    for one logical edit, so feed it into a :class:`Reloader`.
    """

    def __init__(self, paths: List[str], on_change: Callable[[], None]):
        if not have_watchdog:
            raise RuntimeError("the watchdog package is required to watch files for changes")
        self._on_change = on_change
        self._watched_paths: Set[str] = set()
        self._lock = threading.Lock()
        self._pending_directories: Set[str] = set()
        self._retry_task: Optional[RepeatingTask] = None

        directories: List[str] = []
        for path in paths:
            absolute = os.path.abspath(path)
            real_directory = os.path.realpath(os.path.dirname(absolute))
            self._watched_paths.add(os.path.join(real_directory, os.path.basename(absolute)))
            if real_directory not in directories:
                directories.append(real_directory)

        watcher = self

        class _Handler(watchdog.events.FileSystemEventHandler):
            def on_any_event(self, event):
                watcher._handle_event(event)

        self._handler = _Handler()
        self._observer = watchdog.observers.Observer()
        for directory in directories:
            if not self._schedule(directory):
                self._pending_directories.add(directory)
        try:
            self._observer.start()
        except Exception as e:
            log.error("Unable to start watching files for changes: %s", e)
        if len(self._pending_directories) > 0:
            self._retry_task = RepeatingTask.at_interval("ldclient.filedata.watch-retry", _WATCH_RETRY_INTERVAL, _WATCH_RETRY_INTERVAL, self._retry_pending)
            self._retry_task.start()

    def _schedule(self, directory: str) -> bool:
        # The observer accepts a watch on a directory that does not exist and fails later when
        # it starts the watch, so the check happens here first.
        if not os.path.isdir(directory):
            log.warning('Cannot watch directory "%s" for changes yet because it does not exist', directory)
            return False
        try:
            self._observer.schedule(self._handler, directory, recursive=False)
            return True
        except Exception as e:
            log.warning('Cannot watch directory "%s" for changes yet: %s', directory, e)
            return False

    def _retry_pending(self) -> None:
        with self._lock:
            pending = list(self._pending_directories)
        for directory in pending:
            if self._schedule(directory):
                with self._lock:
                    self._pending_directories.discard(directory)
                # Files may have appeared in the directory before the watch was in place.
                self._on_change()
        with self._lock:
            if len(self._pending_directories) == 0 and self._retry_task is not None:
                self._retry_task.stop()

    def _handle_event(self, event) -> None:
        if getattr(event, "event_type", None) not in _CHANGE_EVENT_TYPES:
            return
        candidates = [getattr(event, "src_path", None), getattr(event, "dest_path", None)]
        for candidate in candidates:
            if isinstance(candidate, bytes):
                candidate = candidate.decode("utf-8", errors="replace")
            if candidate in self._watched_paths:
                self._on_change()
                return

    def close(self) -> None:
        """Stops watching and waits briefly for the observer thread to finish."""
        with self._lock:
            if self._retry_task is not None:
                self._retry_task.stop()
        self._observer.stop()
        self._observer.join(timeout=5)
