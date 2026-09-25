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

from typing import List, Optional, Union

from ldclient.config import DataSourceBuilderConfig, OverrideSourceBuilder
from ldclient.impl.integrations.files.filedata import (
    DuplicateKeysHandling,
    abs_file_paths,
    have_watchdog
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
    return the given value for every context. YAML requires the ``pyyaml`` package.

    When multiple files are configured, their entries are combined in the configured order.
    The order determines which file wins under the duplicate keys handling. A reload replaces
    the entire override set, so removing an entry from the files removes the override. A
    configured file that does not exist contributes no overrides: deleting a file removes its
    overrides, and deleting every file removes them all. A file that exists but cannot be read
    or parsed makes that whole reload fail. The previously loaded overrides stay in effect, the
    source logs the failure, retries after a short delay, and recovers on its own once the file
    is readable again.

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

    def __init__(self, paths: Union[str, List[str]]):
        """
        :param paths: the files to load overrides from, as a single path or a list of paths.
          The order is significant: it determines which file wins under the duplicate keys
          handling when the same key appears in more than one file. Relative paths are
          resolved against the current working directory when the source is built.
        """
        self.__paths: List[str] = [paths] if isinstance(paths, str) else list(paths)
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
        """
        self.__poll_interval = seconds
        return self

    def build(self, config: DataSourceBuilderConfig) -> OverrideSource:  # pylint: disable=unused-argument
        """
        Builds the override source. This is called internally by the SDK. It raises
        ``ValueError`` when no file paths were specified or when watching mode was selected
        without the ``watchdog`` package.
        """
        if len(self.__paths) == 0:
            raise ValueError("no file paths were specified for the file-based override source")
        if self.__change_detection == ChangeDetection.WATCHING and not have_watchdog:
            raise ValueError("the file-based override source cannot watch files for changes because the watchdog package is not installed; install it or use polling")

        poll_interval = self.__poll_interval
        if self.__change_detection == ChangeDetection.POLLING and poll_interval < self.MINIMUM_POLL_INTERVAL:
            log.warning("Poll interval %s is below the minimum for the file-based override source; using %s", poll_interval, self.MINIMUM_POLL_INTERVAL)
            poll_interval = self.MINIMUM_POLL_INTERVAL

        return _FileOverrideSource(abs_file_paths(self.__paths), self.__duplicate_keys_handling, self.__change_detection, poll_interval)


__all__ = ['ChangeDetection', 'DuplicateKeysHandling', 'FileOverrideSourceBuilder']
