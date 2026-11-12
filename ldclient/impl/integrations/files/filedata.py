"""
File reading, parsing, and merging logic for components that load flag and segment data from
local files. The file-based override source is built on it. The existing file data sources
keep their own implementation and behavior.

A data file is a JSON or YAML document with optional ``flags``, ``flagValues``, and
``segments`` members. ``flags`` and ``segments`` hold full definitions keyed by key, and a
definition marked as deleted fails the load of its file. ``flagValues`` maps a flag key to a
single value. It expands into a full flag definition that returns that value for every
context. The file name extension decides the format: a file whose name ends in ``.yaml`` or
``.yml`` is YAML, and any other file is JSON.
"""

import codecs
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

# The file name extensions of YAML files, compared without regard to case. Any other file is JSON.
YAML_EXTENSIONS = frozenset([".yaml", ".yml"])

# The watchdog event types that can change a file's content or presence. Opening or reading a
# file also produces events, and a reload reads the files, so those must not count as changes.
# watchdog 7 reports a change of a file's metadata, such as a new modification time from a
# touch, as "attrib", where earlier versions reported "modified". A read does not produce it.
_CHANGE_EVENT_TYPES = frozenset(["created", "modified", "moved", "deleted", "closed", "attrib"])

# How long, in seconds, the watchdog observer waits for further notifications before it delivers
# the ones it has. watchdog 7 on Windows keeps waiting while notifications arrive, so a change is
# reported this late, and not at all while another file in the same directory changes more often
# than this. The watchdog default is one second.
_OBSERVER_TIMEOUT = 0.1


class DuplicateKeysHandling(str, Enum):
    """
    Determines what happens when the same flag or segment key appears in more than one file.
    Flag overrides are currently experimental and subject to change.
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


class DeletedEntryError(FileReadError):
    """
    Indicates that a file holds a flag or segment entry marked as deleted. An override is removed
    by removing its entry, so such a file fails to load like one that cannot be parsed.
    """

    def __init__(self, path: str, kind_name: str, key: str):
        FileDataError.__init__(self, '%s "%s" in %s is marked as deleted; remove the entry to remove its override' % (kind_name, key, path))
        self.path = path
        self.kind_name = kind_name
        self.key = key


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
    for every context. The flag is on, has the value as its only variation, and serves that
    variation as its fallthrough.
    """
    return FeatureFlag({"key": key, "version": 1, "on": True, "fallthrough": {"variation": 0}, "variations": [value]})


def is_yaml_path(path: str) -> bool:
    """
    Whether a data file is a YAML document. The end of the file name decides: a name that ends in
    ``.yaml`` or ``.yml``, in any case, is YAML, including a name that is only that, such as
    ``.yaml``. Any other file, including one with no extension, is JSON.
    """
    return os.path.basename(path).lower().endswith(tuple(YAML_EXTENSIONS))


def read_file(path: str) -> Document:
    """Reads and parses a single data file, in the format its file name extension selects."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError as e:
        raise FileReadError(path, "unable to read file: %s" % e)
    try:
        document = parse_document(raw, is_yaml_path(path))
    except Exception as e:
        raise FileReadError(path, "error parsing file: %s" % e)
    _reject_deleted_entries(path, document)
    return document


def has_content(raw: bytes) -> bool:
    """Whether the raw content of a file holds anything other than white space and a byte order mark."""
    return raw.removeprefix(codecs.BOM_UTF8).strip() != b""


def parse_document(raw: bytes, is_yaml: bool = False) -> Document:
    """
    Parses the raw content of a data file as YAML when ``is_yaml`` is true, which requires the
    ``pyyaml`` package, and as JSON otherwise. The file name extension decides the format; see
    :func:`is_yaml_path`. A byte order mark at the start is ignored.

    Whether a file with no content is valid depends on its format, like the rest of its content.
    A JSON file with no content, or only white space, is not a JSON document, so it fails to
    parse. A YAML file with no content, or with only comments or a document marker, is a
    document with no entries.
    """
    document = _parse_content(raw, is_yaml)
    return Document() if document is None else document


def _parse_content(raw: bytes, is_yaml: bool) -> Optional[Document]:
    """
    The parse behind :func:`parse_document`. Returns None when the YAML parser reads the content
    as nothing, as it does for a file with no content, only comments, or ``null``, rather than as
    a document, even one with no members such as ``{}``. A JSON document must be an object.
    """
    text = raw.decode("utf-8-sig")
    if is_yaml:
        if not have_yaml:
            raise ValueError("the pyyaml package is required to read YAML")
        parsed = yaml.safe_load(text)
    else:
        if not has_content(raw):
            # Not a JSON document. A file that is rewritten in place can be empty for a moment,
            # and the failure keeps the last good data until the file is complete.
            raise ValueError("the file has no content, which is not a JSON document")
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("the document must be an object")
    if parsed is None:
        return None
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
    Validates the shape of a flag or segment entry. The map key is the entry's key for every
    purpose: the entry is stored under it, duplicates are detected by it, and the definition's
    own key is set to it, so that evaluations report the flag by that key. The file format
    allows a definition to omit its version, which the model requires, so a version of 1 is
    filled in.
    """
    if not isinstance(item, dict):
        raise ValueError('%s "%s" must be an object' % (kind_name, key))
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
    merged, _ = _parse_files(_read_files(paths, skip_missing_paths), duplicate_keys_handling)
    return merged


@dataclass
class _FileRead:
    """What one load read from one configured file."""

    path: str
    raw: Optional[bytes]
    """The raw content, or None when the file does not exist and missing files are skipped."""


def _read_files(paths: List[str], skip_missing_paths: bool) -> List[_FileRead]:
    """
    Reads the raw content of all of the given files in order. Raises :class:`FileReadError`
    when a file cannot be read, including a missing file unless ``skip_missing_paths`` is true.
    """
    return [_read_file(path, skip_missing_paths) for path in paths]


def _read_file(path: str, skip_missing_paths: bool) -> _FileRead:
    """Reads the raw content of one file, as :func:`_read_files` does."""
    try:
        with open(path, "rb") as f:
            return _FileRead(path, f.read())
    except FileNotFoundError:
        if skip_missing_paths:
            log.debug("File %s does not exist; it contributes no data", path)
            return _FileRead(path, None)
        raise FileReadError(path, "unable to read file: the file does not exist")
    except OSError as e:
        raise FileReadError(path, "unable to read file: %s" % e)


def _parse_file(path: str, raw: bytes) -> Optional[Document]:
    """Parses what was read from a file that exists. Returns None when the parser read nothing."""
    is_yaml = is_yaml_path(path)
    try:
        document = _parse_content(raw, is_yaml)
    except Exception as e:
        raise FileReadError(path, "error parsing file as %s, the format of its name: %s" % ("YAML" if is_yaml else "JSON", e))
    if document is not None:
        _reject_deleted_entries(path, document)
    return document


def _reject_deleted_entries(path: str, document: Document) -> None:
    """
    Raises :class:`DeletedEntryError` for the first flag or segment entry of a parsed file that is
    marked as deleted. The override layer never holds a deleted item, and an override is removed
    by removing its entry, so such an entry is a mistake in the file that fails its load. The file
    data sources do not use this module and keep accepting such entries.
    """
    for kind_name, items in (("flag", document.flags), ("segment", document.segments)):
        for key, item in items.items():
            if item.deleted:
                raise DeletedEntryError(path, kind_name, key)


def _parse_files(contents: List[_FileRead], duplicate_keys_handling: DuplicateKeysHandling) -> Tuple[MergeResult, bytes]:
    """Parses and merges the content that :func:`_read_files` read."""
    return _merge_files([(read, None if read.raw is None else _parse_file(read.path, read.raw)) for read in contents], duplicate_keys_handling)


def _merge_files(parsed: List[Tuple[_FileRead, Optional[Document]]], duplicate_keys_handling: DuplicateKeysHandling) -> Tuple[MergeResult, bytes]:
    """
    Merges the parsed files. It also returns a digest of the files that were read, so a caller
    can tell whether a later load read the same. One read feeds both the digest and the parse,
    so the digest can never disagree with the content that was parsed. The digest covers each
    path, whether its file exists, and its bytes: the same bytes in a file with another name can
    be in another format, and parse differently.
    """
    documents: List[Document] = []
    files: List[FileSummary] = []
    hasher = hashlib.sha256()
    for read, document in parsed:
        path = os.fsencode(read.path)
        hasher.update(len(path).to_bytes(8, "big"))
        hasher.update(path)
        if read.raw is None:
            hasher.update(b"-")
            files.append(FileSummary(path=read.path))
            continue
        hasher.update(b"+")
        hasher.update(len(read.raw).to_bytes(8, "big"))
        hasher.update(read.raw)
        documents.append(Document() if document is None else document)
        files.append(FileSummary(path=read.path, present=True))

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


FileState = Optional[Tuple[int, int]]


def _observe(path: str) -> FileState:
    """
    Observes the state of a file: its modification time and size, through any symbolic link, or
    None when it does not exist or cannot be examined.
    """
    try:
        info = os.stat(path)
        return (info.st_mtime_ns, info.st_size)
    except OSError:
        return None


def _observe_all(paths: List[str]) -> List[FileState]:
    """Observes the state of each file, as :func:`_observe` does."""
    return [_observe(path) for path in paths]


# A file is held back for at most this many retry delays after a load first held it back, until
# a load holds nothing back.
_MAX_HOLD_RETRY_DELAYS = 5


@dataclass
class _EmptyFile:
    """A file whose content is in effect and that loads have found with no entries."""

    since: Optional[float] = None
    """
    Since when the file has had no entries without being written, on the reloader's clock, or
    None when the last load read it with content, even content that failed to parse, or found
    it missing. A read that fails keeps it, as nothing is known about the content then.
    """
    state: FileState = None
    """The file's state then: its modification time and size, through any symbolic link."""
    first_held: Optional[float] = None
    """
    When a load first held the file back, or None. It bounds how long the file is held back.
    It is kept while loads hold files back, so files emptied in turn stay bounded, and cleared by
    any load that holds nothing back, including one that fails.
    """


class Reloader:
    """
    Owns the reload cycle for a set of data files. It serializes reloads, debounces change
    signals, retains the last good result on failure by not calling ``apply``, retries after
    failures, and skips applications that would change nothing.

    A file can be rewritten in place: a writer truncates it and then writes the new content, and a
    load in between finds it empty. When retries are enabled, a load that finds a file with no
    entries, because it is empty or holds only what its parser reads as nothing (a YAML comment or
    document marker), while that file's content is in effect, holds the file back: it keeps the
    current data without reporting a failure, and the other files' changes wait too, since a load
    applies all files or none. The file is held back until it has stayed so, without being written,
    for the retry delay; a change of its modification time or size starts the delay over. Each file
    is held back for at most five retry delays after a load first held it back, until a load holds
    nothing back, so a file that keeps being rewritten is applied in the end without cutting short
    another file's rewrite. A file whose content is not in effect, such as a placeholder that has
    always been empty, is never held back. Every file is read and parsed, even after another file
    fails, before any is held back, so files emptied together wait together, and a file that cannot
    be read or parsed fails the load, with the first failure, even while another is held back. The
    clock is read after each file's own read, so a slow read of another file does not shorten a
    file's delay.

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
        clock: Callable[[], float] = time.monotonic,
    ):
        """
        :param paths: the files to load, in order. The order determines which file wins under
          the duplicate keys handling.
        :param duplicate_keys_handling: what to do when the same key appears in more than one file
        :param apply: receives each successfully merged result. Calls are serialized. An
          exception from it is reported like a load failure and retried, and the result it
          rejected is not remembered as the last good one.
        :param on_error: receives each distinct failure. Repeats of an identical failure do not
          call it again until a success re-arms it. Failures are also logged here. An exception
          from it is logged and does not prevent the retry.
        :param skip_missing_paths: when true, a configured file that does not exist contributes
          no entries. When false, a missing file fails the load like any other read error.
        :param debounce_delay: how long to wait after a trigger for further triggers to settle
          before reloading. Zero reloads on every trigger.
        :param retry_delay: how long to wait after a failed reload before retrying it
          automatically, and how long a file whose content is in effect must have no entries,
          without being written, before that is applied. Zero disables the automatic retry, and
          an empty file is then parsed as it is.
        :param skip_unchanged: when true, a load whose raw file contents are identical to the
          last applied contents does not call ``apply``
        :param clock: the monotonic clock, in seconds, that times how long files are held back.
          Tests replace it; the timers always use ``time.monotonic``.
        """
        self._paths = list(paths)
        self._duplicate_keys_handling = duplicate_keys_handling
        self._apply = apply
        self._on_error = on_error
        self._skip_missing_paths = skip_missing_paths
        self._debounce_delay = debounce_delay
        self._retry_delay = retry_delay
        self._skip_unchanged = skip_unchanged
        self._clock = clock

        # Guards the deadlines, the closed flag, and the worker start.
        self._cond = threading.Condition()
        self._closed = False
        self._started = False
        self._debounce_deadline: Optional[float] = None
        self._retry_deadline: Optional[float] = None
        # Whether the pending retry was armed by a load that held files back.
        self._retry_after_hold = False

        # Serializes the load work between reload_now and the worker thread. The fields below are
        # only touched while holding it.
        self._reload_lock = threading.Lock()
        self._last_good_digest: Optional[bytes] = None
        self._last_error_message: Optional[str] = None
        # Whether each configured file contributed content to the data in effect. A missing file,
        # or one with no entries, contributed none.
        self._applied_had_content: Dict[str, bool] = {}
        # The files whose content is in effect and that loads have found with no entries. All are
        # forgotten when data is applied or a load is skipped as unchanged, and a load that holds
        # nothing back, even one that fails, starts every file's bound over.
        self._empty_files: Dict[str, _EmptyFile] = {}
        # Whether the last load held files back, which the log line of the retry it arms names.
        self._held_back = False
        # Whether the current load has held a file back, even if it then fails.
        self._holding_this_load = False
        # The delay of the last retry that was armed, for tests.
        self._last_requested_retry_delay: Optional[float] = None

    def reload_now(self) -> None:
        """
        Loads the files synchronously and applies the result or reports the failure. A failure
        arms the same automatic retry as a failed triggered reload.
        """
        self._ensure_started()
        retry_after, held = self._reload()
        if retry_after is not None:
            self._arm_retry(retry_after, keep_sooner=True, after_hold=held)

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

    def _arm_retry(self, retry_after: float, keep_sooner: bool, after_hold: bool) -> None:
        """
        Arms the automatic retry for the given time from now, unless retries are off. With
        ``keep_sooner``, a retry that is already due sooner stays as it is. ``after_hold`` says
        that the load held files back, which the retry's log line names.
        """
        if self._retry_delay <= 0:
            return
        with self._cond:
            if self._closed:
                return
            deadline = time.monotonic() + retry_after
            if keep_sooner and self._retry_deadline is not None and self._retry_deadline <= deadline:
                return
            self._retry_deadline = deadline
            self._retry_after_hold = after_hold
            self._last_requested_retry_delay = retry_after
            self._cond.notify()

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
            after_hold = False
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
                        # it fails or holds files back and arms a fresh retry below.
                        self._debounce_deadline = None
                        self._retry_deadline = None
                        is_retry = False
                    else:
                        self._retry_deadline = None
                        is_retry = True
                        after_hold = self._retry_after_hold
                    break

            if is_retry and after_hold:
                log.debug("Reading flag data again after a file was found empty")
            elif is_retry:
                log.debug("Retrying flag data load after earlier failure")
            else:
                log.info("Reloading flag data after detecting a change")
            try:
                retry_after, held = self._reload()
            except Exception as e:
                log.exception("Unexpected error while reloading flag data: %s", e)
                retry_after, held = None, False
            if retry_after is not None:
                self._arm_retry(retry_after, keep_sooner=False, after_hold=held)

    def _is_closed(self) -> bool:
        with self._cond:
            return self._closed

    def _reload(self) -> Tuple[Optional[float], bool]:
        """
        Performs one full load of all configured files. Returns the time after which to load
        again, or None when the load succeeded, including a skipped no-op application: the retry
        delay after a failure, or the time a hold lasts. Also returns whether the load held files
        back.
        """
        with self._reload_lock:
            if self._is_closed():
                return None, False
            self._held_back = False
            self._holding_this_load = False
            reached: List[str] = []
            retry_after = self._load(reached)
            if not self._holding_this_load:
                # A load that holds nothing back, whether it applies, is skipped or fails, starts the
                # bound over for every file that it read or tried to read, including one that
                # failed to be read or parsed: such a file is no longer held back, so its next
                # rewrite is a new one.
                for path in reached:
                    empty = self._empty_files.get(path)
                    if empty is not None:
                        empty.first_held = None
            return retry_after, self._held_back

    def _load(self, reached: List[str]) -> Optional[float]:
        """
        The body of :meth:`_reload`. It records in ``reached`` each file that it reads or tries to
        read, which is every configured file, since a load reads all of them even after a failure.
        """
        # Every file is read, parsed and classified, even after another file fails, before the
        # load fails, holds files back or merges them. So every file that is found with no
        # entries starts its delay in this load, and every file is reached, so a load that holds
        # nothing back starts every file's bound over. The load fails with the first failure.
        failure: Optional[Exception] = None

        def note_failure(e: Exception) -> None:
            nonlocal failure
            if failure is None:
                failure = e
            else:
                # The load fails with the first failure. The others are logged, so that a second
                # broken file is visible too.
                log.debug("Unable to load flag data, besides the first failure: %s", e)

        held_back: List[str] = []
        parsed: List[Tuple[_FileRead, Optional[Document]]] = []
        had_content: Dict[str, bool] = {}
        for path in self._paths:
            reached.append(path)
            try:
                read = _read_file(path, self._skip_missing_paths)
            except Exception as e:
                # Nothing is known about the content, so the file's empty state, if any, stays.
                note_failure(e)
                continue
            if read.raw is None:
                # A missing file is not an empty one.
                self._end_empty(read.path)
                had_content[read.path] = False
                parsed.append((read, None))
                continue
            # A file with no content, or with content that its parser reads as nothing, such as
            # YAML with only a comment, has no document and so no entries. A read can see that
            # much of a file that is being written. Content with no document is invalid JSON, so
            # it is only parsed when the file is not held back. {} is a document.
            document: Optional[Document] = None
            has_document = has_content(read.raw)
            if has_document:
                try:
                    document = _parse_file(read.path, read.raw)
                except FileReadError as e:
                    # Content that cannot be parsed is not an empty file either.
                    self._end_empty(read.path)
                    note_failure(e)
                    continue
                has_document = document is not None
            if has_document:
                self._end_empty(read.path)
            # The clock is read after this file's own read, so that a slow read of another file
            # does not shorten its delay.
            elif self._should_hold_back(read.path, self._clock()):
                held_back.append(read.path)
                self._holding_this_load = True
                continue
            # The file was read and is not held back, even if it then fails to parse.
            had_content[read.path] = has_document
            if not has_content(read.raw):
                try:
                    document = _parse_file(read.path, read.raw)
                except FileReadError as e:
                    note_failure(e)
                    continue
            parsed.append((read, document))
        if failure is not None:
            return self._fail(failure)
        if len(held_back) > 0:
            return self._hold_back(held_back, self._clock())

        try:
            merged, digest = _merge_files(parsed, self._duplicate_keys_handling)
        except Exception as e:
            return self._fail(e)

        # A close may have happened while the files were being read. Deliver nothing then.
        if self._is_closed():
            return None

        # A success right after a failure applies even when the content is unchanged since the
        # last success. The consumer heard about the failure and only an application tells it
        # that things are good again.
        recovering = self._last_error_message is not None
        if self._skip_unchanged and not recovering and digest == self._last_good_digest:
            # The data in effect is confirmed, as by an application, so it is remembered the same
            # way, and the bounds start over.
            self._remember_applied(had_content)
            return None

        # Nothing is remembered until the consumer has accepted the result. A result the
        # consumer rejects must not become the baseline that skip-unchanged compares against,
        # and must not count as a recovery.
        try:
            self._apply(merged)
        except Exception as e:
            return self._fail(e)
        self._last_error_message = None
        self._last_good_digest = digest
        self._remember_applied(had_content)
        return None

    def _end_empty(self, path: str) -> None:
        """
        Ends the empty state of a file that a load read with content, even content that cannot be
        parsed, or found missing, so that it starts a new delay when it is found empty again. Its
        bound is kept until a load holds nothing back.
        """
        empty = self._empty_files.get(path)
        if empty is not None:
            empty.since = None

    def _remember_applied(self, had_content: Dict[str, bool]) -> None:
        """Records what each file contributed to the data in effect, and forgets the empty files."""
        self._applied_had_content.update(had_content)
        self._empty_files.clear()

    def _should_hold_back(self, path: str, now: float) -> bool:
        """
        Decides whether a file that a load found with no entries is held back. A file whose
        content is in effect and that has none now may be in the middle of an in-place rewrite.
        It is held back until it has stayed so, without being written, for the retry delay. A
        change of its state, its modification time and size read through any symbolic link,
        means that it was written since a load last found it so, for example rewritten and
        truncated again between two loads, so the delay starts over. A file that has stayed so
        for the whole delay is not held back. Each file is held back for at most five retry
        delays after a load first held it back, so that a file that keeps being written while
        every load finds it empty, or files that are emptied in turn, are not held back without
        end. The bound starts over only at a load that holds nothing back, for every file, whether
        the load applies, is skipped or fails. A load reads every file even after one fails. A file whose content is not in effect
        is parsed right away, and so is any file when retries are off, since nothing would read it
        again.
        """
        if self._retry_delay <= 0 or not self._applied_had_content.get(path, False):
            return False
        state = _observe(path)
        empty = self._empty_files.get(path)
        if empty is None:
            empty = _EmptyFile()
            self._empty_files[path] = empty
        if empty.since is None or empty.state != state:
            empty.since = now
            empty.state = state
        if now - empty.since >= self._retry_delay:
            # The file stayed empty for the whole delay. Its bound starts over only at a load that
            # holds nothing back, so that files emptied in turn do not keep each other held back.
            return False
        if empty.first_held is not None and now - empty.first_held >= _MAX_HOLD_RETRY_DELAYS * self._retry_delay:
            return False
        if empty.first_held is None:
            empty.first_held = now
        return True

    def _hold_back(self, held_back: List[str], now: float) -> Optional[float]:
        """
        Keeps the current data for a load that holds files back. Whatever the files' format,
        nothing is applied or reported. The returned time arms the retry for when the first of
        the files has stayed so for the retry delay, or has been held back for as long as it may
        be, plus a tenth of the retry delay, so that a timer that fires a little early still
        finds the time over. The change signal for a write starts a load sooner.
        """
        if self._is_closed():
            return None
        until = float("inf")
        for path in held_back:
            log.debug("File %s has no entries, possibly while it is being rewritten; waiting for it to be written before applying it", path)
            empty = self._empty_files[path]
            since = now if empty.since is None else empty.since
            first_held = now if empty.first_held is None else empty.first_held
            until = min(until, since + self._retry_delay, first_held + _MAX_HOLD_RETRY_DELAYS * self._retry_delay)
        self._held_back = True
        return max(until - now, 0.0) + self._retry_delay / 10

    def _fail(self, err: Exception) -> Optional[float]:
        if self._is_closed():
            return None
        # With automatic retries, a persistent failure would repeat the same log entry and the
        # same callback on every attempt. Repeats of an identical failure are logged at debug
        # level and do not call on_error again.
        message = str(err)
        if message == self._last_error_message:
            log.debug("Unable to load flag data: %s", err)
            return self._retry_delay
        self._last_error_message = message
        log.error("Unable to load flag data: %s", err)
        if self._on_error is not None:
            try:
                self._on_error(err)
            except Exception as e:
                # A consumer that raises while it reports a failure must not stop the retry.
                log.error("Error while reporting a flag data load failure: %s", e)
        return self._retry_delay


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


# The most symbolic links followed from one configured file, as in the operating system's own
# limit, so that a loop of links ends.
_MAX_LINKS = 40


def _link_target(path: str) -> Optional[str]:
    """
    The path of the file that a configured symbolic link finally points to, with its directory
    resolved to a real path, or None when the configured file is not a link, the target's
    directory does not exist, or the links loop. The target file itself need not exist, so that
    a link created ahead of its target is followed once the target appears.

    A relative link is relative to the real directory that holds it, so it is joined to that
    directory's real path. Normalizing the path as written would take a ".." to the wrong place
    when a directory in that path is itself a link.
    """
    try:
        if not os.path.islink(path):
            return None
        target = path
        for _ in range(_MAX_LINKS):
            directory = os.path.dirname(target)
            if not os.path.isdir(directory):
                return None
            real_directory = os.path.realpath(directory)
            if not os.path.islink(target):
                return os.path.join(real_directory, os.path.basename(target))
            target = os.path.join(real_directory, os.readlink(target))
        return None
    except (OSError, ValueError):
        return None


def _event_path(value: Any) -> Optional[str]:
    """The path carried by a watchdog event as text, or None when the event has none."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return None


class Watcher:
    """
    Detects changes to a set of files through file system change notifications, using the
    ``watchdog`` package. The directory of each file is watched, so a file that does not exist
    yet is picked up when it appears.

    A directory that cannot be watched, for example because it does not exist yet or because
    the notification mechanism cannot be started, is attempted again on an interval. A watched
    directory that is deleted has its watch dropped and set up again once the directory exists.
    When a retry sets up a watch, the callback runs once, so that changes made while the watch
    was not in place are picked up.

    A configured file can be a symbolic link to a file in another directory. The directory of
    the link's target is watched as well, for the target's path, so that an edit to the target
    is reported. The links are resolved again after every reported change, after a retry sets up
    watches, and when a directory that holds a target is deleted, as happens when a Kubernetes
    ConfigMap volume is updated. A link that cannot be resolved, for example because its target's
    directory does not exist yet or was deleted, is resolved again on the retry interval until
    its target is watched, and the callback then runs once.

    A change that none of those events reports is not followed: a link further along the chain
    that is pointed elsewhere while its old target stays, a configured directory that is itself
    a link and is pointed elsewhere, or a configured directory that is renamed and replaced.
    Deployments that switch files that way, such as a ``current`` link to a release directory,
    should use polling, which reads every configured path on each interval.

    Notifications for the watched paths invoke the callback. The callback can run several times
    for one logical edit, so feed it into a :class:`Reloader`.
    """

    def __init__(self, paths: List[str], on_change: Callable[[], None]):
        if not have_watchdog:
            raise RuntimeError("the watchdog package is required to watch files for changes")
        self._on_change = on_change
        self._paths = [os.path.abspath(path) for path in paths]
        self._watched_paths: Set[str] = set()
        self._directories: Set[str] = set()
        for absolute in self._paths:
            real_directory = os.path.realpath(os.path.dirname(absolute))
            self._watched_paths.add(os.path.join(real_directory, os.path.basename(absolute)))
            self._directories.add(real_directory)

        watcher = self

        class _Handler(watchdog.events.FileSystemEventHandler):
            def on_any_event(self, event):
                watcher._handle_event(event)

        self._handler = _Handler()
        self._observer = watchdog.observers.Observer(timeout=_OBSERVER_TIMEOUT)

        # Guards the state below. It is never held while the observer is called, because the
        # observer holds its own lock while it dispatches an event to the handler.
        self._lock = threading.Lock()
        self._closed = False
        self._observer_running = False
        # The watch on each directory that currently has one.
        self._watches: Dict[str, Any] = {}
        # The directories that have no watch yet. The retry schedule runs while this is not empty.
        self._pending_directories: Set[str] = set(self._directories)
        self._retry_task: Optional[RepeatingTask] = None
        # Serializes the watch setup against close, so that a setup in progress cannot start
        # anything after close has stopped the observer.
        self._setup_lock = threading.Lock()
        # The files that the configured symbolic links point to, and the directories that are
        # watched only because a target is in them. A directory that is also configured is in
        # _directories and keeps its own watch.
        self._target_paths: Set[str] = set()
        self._target_directories: Set[str] = set()
        # True while a configured link's target is not watched, because the link cannot be
        # resolved or its target's directory cannot be watched. The retry schedule runs while it
        # is true.
        self._links_unresolved = False
        # The last reason logged for each directory that cannot be watched yet, so that a retry
        # that fails the same way logs at debug level. A directory leaves it once it is watched.
        self._unwatchable_messages: Dict[str, str] = {}

        self._set_up_watches(is_retry=False)
        self._refresh_links()

    def _set_up_watches(self, is_retry: bool) -> None:
        """
        Starts the observer when it is not running, then sets up a watch on each pending
        directory. Whatever cannot be set up stays pending and is attempted again on the retry
        schedule. When a retry sets up a watch, the callback runs once, because the files may
        have changed while the watch was not in place.
        """
        with self._setup_lock:
            with self._lock:
                if self._closed:
                    return
                running = self._observer_running
            if not running:
                try:
                    self._observer.start()
                except Exception as e:
                    log.error("Unable to start watching files for changes: %s", e)
                    self._ensure_retry_scheduled()
                    return
                with self._lock:
                    self._observer_running = True

            with self._lock:
                pending = sorted(self._pending_directories)
            armed = False
            for directory in pending:
                watch = self._schedule(directory)
                if watch is None:
                    continue
                with self._lock:
                    self._pending_directories.discard(directory)
                    self._watches[directory] = watch
                armed = True

            with self._lock:
                if len(self._pending_directories) > 0:
                    self._ensure_retry_scheduled_locked()
        if is_retry and armed:
            self._refresh_links()
            self._on_change()

    def _schedule(self, directory: str) -> Optional[Any]:
        """Sets up a watch on a directory. Returns the watch, or None when the directory cannot be watched yet."""
        # The observer accepts a watch on a directory that does not exist and fails later when
        # it starts the watch, so the check happens here first.
        if not os.path.isdir(directory):
            self._log_unwatchable(directory, 'Cannot watch directory "%s" for changes yet because it does not exist' % directory)
            return None
        try:
            watch = self._observer.schedule(self._handler, directory, recursive=False)
        except Exception as e:
            self._log_unwatchable(directory, 'Cannot watch directory "%s" for changes yet: %s' % (directory, e))
            return None
        # watchdog 6 on Windows starts a watch's reader thread and returns before the thread has
        # asked for its first notification, and a change made before then is not reported.
        # Yielding lets the thread make that request before the setup goes on, for example to
        # the initial load.
        time.sleep(0)
        with self._lock:
            self._unwatchable_messages.pop(directory, None)
        return watch

    def _log_unwatchable(self, directory: str, message: str) -> None:
        """
        Logs why a directory cannot be watched. The retry attempts it again every interval, so
        an unchanged reason for the same directory is logged at debug level after the first time,
        until the directory is watched.
        """
        with self._lock:
            repeated = self._unwatchable_messages.get(directory) == message
            self._unwatchable_messages[directory] = message
        if repeated:
            log.debug(message)
        else:
            log.warning(message)

    def _ensure_retry_scheduled(self) -> None:
        with self._lock:
            self._ensure_retry_scheduled_locked()

    def _ensure_retry_scheduled_locked(self) -> None:
        if self._closed or self._retry_task is not None:
            return
        self._retry_task = RepeatingTask.at_interval("ldclient.filedata.watch-retry", _WATCH_RETRY_INTERVAL, _WATCH_RETRY_INTERVAL, self._retry_pending)
        self._retry_task.start()

    def _retry_pending(self) -> None:
        self._set_up_watches(is_retry=True)
        with self._lock:
            links_unresolved = self._links_unresolved
        if links_unresolved and self._refresh_links():
            # The target that a retry watches may have changed while it was not watched.
            self._on_change()
        with self._lock:
            # The schedule ends once everything is watched, the targets of the configured links
            # included. It is set up again when a directory becomes pending or a link cannot be
            # resolved later. The check and the stop happen under the lock, so a directory that
            # becomes pending at the same time either keeps this schedule or starts a new one.
            if len(self._pending_directories) == 0 and not self._links_unresolved and self._observer_running and self._retry_task is not None:
                self._retry_task.stop()
                self._retry_task = None

    def _directory_lost(self, directory: str) -> None:
        """
        Drops the watch on a directory that no longer exists and arms the retry that sets it up
        again once the directory exists.
        """
        with self._lock:
            if self._closed:
                return
            stale = self._watches.pop(directory, None)
            self._pending_directories.add(directory)
            self._ensure_retry_scheduled_locked()
        log.warning('Directory "%s" no longer exists. It is watched again when it exists.', directory)
        self._unschedule(directory, stale)
        with self._lock:
            held_target = any(os.path.dirname(target) == directory for target in self._target_paths)
        if held_target:
            # The file that a configured link names has changed.
            self._refresh_links()
            self._on_change()

    def _target_directory_lost(self, directory: str) -> None:
        """
        Drops the watch on a directory that held only link targets, resolves the links again,
        and runs the callback, since the file that a configured link names has changed.
        """
        with self._lock:
            if self._closed:
                return
            stale = self._watches.pop(directory, None)
            self._target_directories.discard(directory)
        log.debug('Directory "%s" of a linked data file no longer exists. The links are resolved again.', directory)
        self._unschedule(directory, stale)
        self._refresh_links()
        self._on_change()

    def _unschedule(self, directory: str, watch: Optional[Any]) -> None:
        if watch is None:
            return
        # The observer keeps a watch whose directory is gone, and a later watch on the same path
        # would reuse it, so it is removed.
        try:
            self._observer.unschedule(watch)
        except Exception as e:
            log.debug('Unable to remove the watch on directory "%s": %s', directory, e)

    def _refresh_links(self) -> bool:
        """
        Resolves each configured file that is a symbolic link and watches the directory of each
        target that is not a configured directory, which has a watch of its own. A directory that
        no longer holds a target of a configured link loses its watch. A link that cannot be
        resolved, for example because its target's directory does not exist yet, or whose
        target's directory cannot be watched, keeps the retry schedule running, which resolves
        the links again. Returns True if it set up a watch or if a link now leads to a target it
        did not lead to before, which can be in a directory that is already watched.

        This runs on the observer's thread too, which holds the observer's lock, so it takes only
        the watcher's own lock and never while the observer is called.
        """
        targets = set()
        unresolved = []
        for path in self._paths:
            target = _link_target(path)
            if target is not None:
                targets.add(target)
            elif os.path.islink(path):
                unresolved.append(path)
        directories = {os.path.dirname(target) for target in targets} - self._directories
        with self._lock:
            if self._closed:
                return False
            new_targets = targets - self._target_paths
            self._target_paths = targets
            added = sorted(d for d in directories if d not in self._watches)
            removed = [d for d in self._target_directories if d not in directories]
            self._target_directories = set(directories)
            stale = [(d, self._watches.pop(d, None)) for d in removed]
        for directory, watch in stale:
            self._unschedule(directory, watch)
        watched_any = False
        unwatched = False
        for directory in added:
            watch = self._schedule(directory)
            if watch is None:
                unwatched = True
                continue
            with self._lock:
                stored = self._watches.get(directory)
                keep = not self._closed and directory in self._target_directories and stored is None
                if keep:
                    self._watches[directory] = watch
            if keep:
                watched_any = True
            elif stored is None or stored != watch:
                # The observer shares one watch per directory, so a watch that another thread
                # stored for this directory meanwhile is this one, and removing it would end it.
                self._unschedule(directory, watch)
        if unresolved:
            log.debug('Cannot resolve the target of %s yet. The links are resolved again shortly.', ", ".join(unresolved))
        with self._lock:
            if self._closed:
                return watched_any
            self._links_unresolved = bool(unresolved) or unwatched
            if self._links_unresolved:
                self._ensure_retry_scheduled_locked()
        return watched_any or len(new_targets) > 0

    def _handle_event(self, event) -> None:
        event_type = getattr(event, "event_type", None)
        if event_type not in _CHANGE_EVENT_TYPES:
            return
        src_path = _event_path(getattr(event, "src_path", None))
        if event_type in ("deleted", "moved") and getattr(event, "is_directory", False):
            if src_path in self._directories:
                self._directory_lost(src_path)
                return
            with self._lock:
                is_target_directory = src_path is not None and src_path in self._target_directories
            if src_path is not None and is_target_directory:
                self._target_directory_lost(src_path)
                return
        with self._lock:
            target_paths = self._target_paths
        for candidate in (src_path, _event_path(getattr(event, "dest_path", None))):
            if candidate in self._watched_paths or candidate in target_paths:
                # The change can be to a configured link itself, so the links are resolved again
                # first.
                self._refresh_links()
                self._on_change()
                return

    def close(self) -> None:
        """Stops watching and waits briefly for the observer thread to finish."""
        with self._setup_lock:
            with self._lock:
                self._closed = True
                retry_task = self._retry_task
                self._retry_task = None
                running = self._observer_running
            if retry_task is not None:
                retry_task.stop()
            self._observer.stop()
            # A thread that never started cannot be joined.
            if running:
                self._observer.join(timeout=5)
