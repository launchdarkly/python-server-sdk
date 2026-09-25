"""
The file-based override source. It reads flag and segment overrides from one or more local
files and reloads them when the files change.
"""

import threading
from enum import Enum
from typing import List, Optional, Union

from ldclient.impl.integrations.files.filedata import (
    DEFAULT_DEBOUNCE_DELAY,
    DEFAULT_RETRY_DELAY,
    DuplicateKeysHandling,
    FileSummary,
    MergeResult,
    Poller,
    Reloader,
    Watcher
)
from ldclient.impl.util import log
from ldclient.interfaces import OverrideSink, OverrideSource


class ChangeDetection(str, Enum):
    """
    Selects how the file-based override source learns that a file changed. The two modes are
    alternatives. Flag overrides are currently experimental and subject to change.
    """

    POLLING = "polling"
    """
    The source examines the files on a fixed interval and reloads when the modification time
    or the size of a file changes. Polling works on every file system, including network
    mounts and directories whose contents are swapped through symbolic links, as Kubernetes
    does for mounted ConfigMaps. It is the default.
    """

    WATCHING = "watching"
    """
    The source reloads in response to file system change notifications, using the ``watchdog``
    package. It reacts faster than polling. It depends on notifications, which some file
    systems do not deliver reliably.
    """


class _FileOverrideSource(OverrideSource):
    """
    Reads overrides from the configured files and supplies each successful load to the sink as
    a full snapshot. A configured file that does not exist contributes no overrides. A file
    that cannot be read or parsed fails that load, the last good snapshot stays in effect, and
    the load is retried.
    """

    def __init__(self, paths: List[str], duplicate_keys_handling: DuplicateKeysHandling, change_detection: ChangeDetection, poll_interval: float):
        self._paths = paths
        self._duplicate_keys_handling = duplicate_keys_handling
        self._change_detection = change_detection
        self._poll_interval = poll_interval
        self._reloader: Optional[Reloader] = None
        self._change_detector: Optional[Union[Poller, Watcher]] = None
        self._lock = threading.Lock()
        self._closed = False

    def start(self, sink: OverrideSink) -> None:
        def apply(merged: MergeResult) -> None:
            sink.set_overrides(merged.flags, merged.segments)
            _log_overrides_in_effect(merged)

        reloader = Reloader(
            self._paths,
            self._duplicate_keys_handling,
            apply=apply,
            skip_missing_paths=True,
            debounce_delay=DEFAULT_DEBOUNCE_DELAY,
            retry_delay=DEFAULT_RETRY_DELAY,
            skip_unchanged=True,
        )
        with self._lock:
            if self._closed:
                return
            self._reloader = reloader

        # The initial load runs synchronously, so overrides present in the files are in effect
        # by the time the client constructor returns. A file that does not exist yet
        # contributes no overrides. A file that cannot be read or parsed is not fatal: the
        # client runs with no overrides, the failure is logged, and the retry recovers once
        # the file is readable.
        reloader.reload_now()

        with self._lock:
            if self._closed:
                return
            if self._change_detection == ChangeDetection.WATCHING:
                self._change_detector = Watcher(self._paths, reloader.trigger)
            else:
                poller = Poller(self._paths, self._poll_interval, reloader.trigger)
                poller.start()
                self._change_detector = poller

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            change_detector = self._change_detector
            reloader = self._reloader
            self._change_detector = None
            self._reloader = None
        if change_detector is not None:
            change_detector.close()
        if reloader is not None:
            reloader.close()


def _log_overrides_in_effect(merged: MergeResult) -> None:
    """
    Reports the overrides now in effect and what each configured file supplied. The reloader
    applies a snapshot only when the content changed, so this logs each change once.
    """
    details = "; ".join(_file_summary_text(summary) for summary in merged.files)
    if len(merged.flags) == 0 and len(merged.segments) == 0:
        log.info("Flag overrides: none in effect (%s)", details)
        return
    log.info("Flag overrides in effect: %s (%s)", _counts_text(len(merged.flags), len(merged.segments)), details)


def _file_summary_text(summary: FileSummary) -> str:
    if not summary.present:
        return "%s: absent" % summary.path
    if summary.flags == 0 and summary.segments == 0:
        return "%s: no entries" % summary.path
    return "%s: %s" % (summary.path, _counts_text(summary.flags, summary.segments))


def _counts_text(flags: int, segments: int) -> str:
    """Formats flag and segment counts, for example "2 flags, 1 segment"."""
    parts = []
    if flags > 0:
        parts.append(_pluralize(flags, "flag"))
    if segments > 0:
        parts.append(_pluralize(segments, "segment"))
    return ", ".join(parts)


def _pluralize(count: int, noun: str) -> str:
    if count == 1:
        return "1 %s" % noun
    return "%d %ss" % (count, noun)
