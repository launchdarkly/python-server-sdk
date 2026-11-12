"""
Override sources for the SDK's flag override capability. Flag overrides are currently
experimental and subject to change.

Overrides are flag and segment definitions that take precedence over data received from
LaunchDarkly at evaluation time, on a per-key basis. They exist for resilience during an
incident. An operator can force one or more flags to a known state on a running application,
whether or not the application can reach LaunchDarkly. The override stays in effect until the
operator removes it. Flags not present in the override data are completely unaffected.

This module currently provides one source: :class:`FileOverrideSourceBuilder`, which reads
overrides from local files and reloads them as the files change. Configure it with the data
system builder:
::

    from ldclient import Config, datasystem
    from ldclient.integrations.overrides import FileOverrideSourceBuilder

    source = FileOverrideSourceBuilder(['/etc/launchdarkly/overrides.json'])
    config = Config(sdk_key, datasystem_config=datasystem.default().overrides(source).build())

An evaluation that an override affects is marked. The marking is direct or transitive. It
applies when the evaluated flag, a prerequisite at any depth, or a segment read during the
evaluation came from the override layer. The evaluation reason's ``overrideAffected`` property
reports the marking. Marked evaluations appear in analytics summary events only, under separate
counters, so LaunchDarkly can distinguish them. They produce no individual evaluation events.
"""

import math
import os
import threading
from typing import List, Optional, Sequence, Union

from ldclient.config import DataSourceBuilderConfig, OverrideSourceBuilder
from ldclient.impl.integrations.files.filedata import (
    DuplicateKeysHandling,
    abs_file_paths,
    have_watchdog,
    have_yaml,
    is_yaml_path
)
from ldclient.impl.integrations.overrides.file_override_source import (
    ChangeDetection,
    _FileOverrideSource
)
from ldclient.impl.util import log
from ldclient.interfaces import OverrideSource


class FileOverrideSourceBuilder(OverrideSourceBuilder):
    """
    A builder for a file-based override source. Flag overrides are currently experimental and
    subject to change.

    The source reads flag and segment overrides from one or more local files and reloads them
    as the files change. The files use the same document format as the file data source:
    each file is a JSON or YAML document with optional ``flags``, ``flagValues``, and
    ``segments`` members. ``flagValues`` entries are expanded into full flag definitions that
    return the given value for every context. A ``flagValues`` entry is not available to
    client-side SDKs, so :meth:`ldclient.client.LDClient.all_flags_state` with
    ``client_side_only=True`` leaves it out, even when the flag it overrides is available to them.
    To override such a flag, define it in ``flags`` with ``"clientSide": true``.

    The file name decides the format of each file: a file whose name ends in ``.yaml`` or
    ``.yml`` is YAML, which requires the ``pyyaml`` package, and any other file is JSON. Files
    must be UTF-8.

    When multiple files are configured, their entries are combined in the configured order.
    The order determines which file wins under the duplicate keys handling. A reload replaces
    the entire override set, so removing an entry from the files removes the override. A
    configured file that does not exist contributes no overrides: deleting a file removes its
    overrides, and deleting every file removes them all. A file that exists but cannot be read
    or parsed makes that whole reload fail. The previously loaded overrides stay in effect, the
    source logs the failure, retries after a short delay, and recovers on its own once the file
    is readable again. A file that contains an entry marked as deleted fails to load, and the
    overrides in effect stay; to remove an override, remove its entry or delete the file.

    Files can be rewritten in place: if the source finds a file empty while it is being
    rewritten, it keeps the overrides in effect until the new content is written. To remove a
    file's overrides, delete the file or write a document with no entries, such as ``{}``. An
    empty JSON file is not a valid document. A partly written YAML file can still parse, so for a
    YAML file, replacing its contents in one operation, such as by renaming a fully written
    temporary file over it, avoids briefly applying a partly written file.

    Whenever the set of overrides in effect changes, including at startup, the source logs the
    overrides in effect and what each configured file supplied, at Info level.

    By default the source polls the files for changes once per second. See
    :meth:`change_detection` and :meth:`poll_interval`.
    """

    DEFAULT_POLL_INTERVAL = 1.0
    """
    The interval, in seconds, at which the source examines the files for changes in polling
    mode when no interval was specified. Because the source reads local files rather than
    contacting a service, a short interval keeps an override responsive during an incident at
    negligible cost.
    """

    MINIMUM_POLL_INTERVAL = 1.0
    """
    The shortest allowed polling interval, in seconds. A configured interval below this is
    raised to it. The minimum exists only to prevent a pathological tight loop over the file
    system.
    """

    def __init__(self, paths: Union[str, 'os.PathLike[str]', Sequence[Union[str, 'os.PathLike[str]']]]):
        """
        :param paths: the files to load overrides from, as a single path or a list of paths.
          Each path is a string or a path object such as :class:`pathlib.Path`. The order is
          significant: it determines which file wins under the duplicate keys handling when the
          same key appears in more than one file. Relative paths are resolved against the
          current working directory when the source is built.
        :raises TypeError: when a path is neither a string nor a path object that names a string
          path
        """
        # bytes and bytearray are single values too, so that they get the error below instead of
        # being read as a sequence of integers.
        if isinstance(paths, (str, bytes, bytearray, os.PathLike)):
            paths = [paths]
        self.__paths: List[str] = []
        for path in paths:
            try:
                name: object = os.fspath(path)
            except TypeError:
                name = None
            if not isinstance(name, str):
                raise TypeError("file paths for the file-based override source must be strings or path objects; got %r" % (path,))
            self.__paths.append(name)
        self.__duplicate_keys_handling = DuplicateKeysHandling.FAIL
        self.__change_detection = ChangeDetection.POLLING
        self.__poll_interval = self.DEFAULT_POLL_INTERVAL

    def duplicate_keys_handling(self, handling: Union[DuplicateKeysHandling, str]) -> 'FileOverrideSourceBuilder':
        """
        Specifies how to handle the same key appearing in more than one file. The default is
        :attr:`DuplicateKeysHandling.FAIL`, which treats the reload as failed and keeps the
        previously loaded overrides. :attr:`DuplicateKeysHandling.IGNORE` keeps the entry from
        the first configured file that defines the key and discards the others.

        :param handling: the handling, as the enum or its string value (``"fail"`` or ``"ignore"``)
        """
        self.__duplicate_keys_handling = DuplicateKeysHandling(handling)
        return self

    def change_detection(self, mode: Union[ChangeDetection, str]) -> 'FileOverrideSourceBuilder':
        """
        Selects how the source detects file changes. The default is
        :attr:`ChangeDetection.POLLING`. The two modes are alternatives, so setting one replaces
        the other. :attr:`ChangeDetection.WATCHING` requires the ``watchdog`` package.

        Watching follows edits to the configured files and to the targets of configured symbolic
        links, including the update of a mounted Kubernetes ConfigMap. It does not follow a
        deployment that points a link further along the path, or a configured directory, at
        another location, such as a ``current`` link to a release directory, or that replaces a
        configured directory. Use polling for such deployments.

        :param mode: the mode, as the enum or its string value (``"polling"`` or ``"watching"``)
        """
        self.__change_detection = ChangeDetection(mode)
        return self

    def poll_interval(self, seconds: float) -> 'FileOverrideSourceBuilder':
        """
        Sets the interval between examinations of the files in polling mode. Watching mode
        ignores it. The default is :attr:`DEFAULT_POLL_INTERVAL`. An interval below
        :attr:`MINIMUM_POLL_INTERVAL` is raised to the minimum.

        :param seconds: the interval in seconds
        :raises TypeError: when the interval is not a number
        :raises ValueError: when the interval is not a finite number, or is longer than the
            runtime can wait
        """
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError("the poll interval must be a number of seconds")
        if not math.isfinite(seconds):
            raise ValueError("the poll interval must be a finite number of seconds")
        if seconds > threading.TIMEOUT_MAX:
            raise ValueError("the poll interval must not exceed %s seconds" % threading.TIMEOUT_MAX)
        self.__poll_interval = seconds
        return self

    def build(self, config: DataSourceBuilderConfig) -> OverrideSource:  # pylint: disable=unused-argument
        """
        Builds the override source. This is called internally by the SDK. It raises
        ``ValueError`` when no file paths were specified, when watching mode was selected
        without the ``watchdog`` package, or when a YAML file was specified without the
        ``pyyaml`` package.
        """
        if len(self.__paths) == 0:
            raise ValueError("no file paths were specified for the file-based override source")
        if self.__change_detection == ChangeDetection.WATCHING and not have_watchdog:
            raise ValueError("the file-based override source cannot watch files for changes because the watchdog package is not installed; install it or use polling")
        if not have_yaml:
            for path in self.__paths:
                if is_yaml_path(path):
                    raise ValueError("the file-based override source cannot read the YAML file %s because the pyyaml package is not installed; install it or use JSON" % path)

        poll_interval = self.__poll_interval
        if self.__change_detection == ChangeDetection.POLLING and poll_interval < self.MINIMUM_POLL_INTERVAL:
            log.warning("Poll interval %s is below the minimum for the file-based override source; using %s", poll_interval, self.MINIMUM_POLL_INTERVAL)
            poll_interval = self.MINIMUM_POLL_INTERVAL

        # A path listed more than once would make every entry of that file a duplicate of itself,
        # so the repeats are dropped and the first occurrence keeps its place in the order. Paths
        # are compared the way the platform compares file names: without regard to case on
        # Windows, exactly elsewhere.
        all_paths = abs_file_paths(self.__paths)
        paths: List[str] = []
        seen = set()
        for path in all_paths:
            key = os.path.normcase(path)
            if key not in seen:
                seen.add(key)
                paths.append(path)
        if len(paths) != len(all_paths):
            log.warning("The file-based override source lists the same file more than once; the repeats are ignored")

        return _FileOverrideSource(paths, self.__duplicate_keys_handling, self.__change_detection, poll_interval)


__all__ = ['ChangeDetection', 'DuplicateKeysHandling', 'FileOverrideSourceBuilder']
