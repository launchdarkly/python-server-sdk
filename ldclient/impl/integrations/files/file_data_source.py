import time
import traceback
from typing import Optional, Union

from ldclient.impl.integrations.files.filedata import (
    DEFAULT_DEBOUNCE_DELAY,
    DEFAULT_RETRY_DELAY,
    DuplicateKeysHandling,
    MergeResult,
    Poller,
    Reloader,
    Watcher,
    abs_file_paths,
    have_watchdog
)
from ldclient.impl.util import log
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceUpdateSink,
    UpdateProcessor
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class _FileDataSource(UpdateProcessor):
    def __init__(self, store, data_source_update_sink: Optional[DataSourceUpdateSink], ready, paths, auto_update, poll_interval, force_polling):
        self._store = store
        self._data_source_update_sink = data_source_update_sink
        self._ready = ready
        self._inited = False
        self._paths = abs_file_paths(paths if isinstance(paths, list) else [paths])
        self._auto_update = auto_update
        self._auto_updater: Optional[Union[Poller, Watcher]] = None
        self._poll_interval = poll_interval
        self._force_polling = force_polling
        # Debouncing and automatic retries only matter when something can trigger further
        # reloads. A source without auto update loads exactly once.
        self._reloader = Reloader(
            self._paths,
            DuplicateKeysHandling.FAIL,
            apply=self._apply,
            on_error=self._handle_error,
            debounce_delay=DEFAULT_DEBOUNCE_DELAY if auto_update else 0.0,
            retry_delay=DEFAULT_RETRY_DELAY if auto_update else 0.0,
            skip_unchanged=True,
        )

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
        self._reloader.reload_now()

        if self._auto_update:
            self._auto_updater = self._start_auto_updater()

        # We will signal readiness immediately regardless of whether the file load succeeded or failed -
        # the difference can be detected by checking initialized()
        self._ready.set()

    def stop(self):
        if self._auto_updater is not None:
            self._auto_updater.close()
            self._auto_updater = None
        self._reloader.close()

    def initialized(self):
        return self._inited

    def _apply(self, merged: MergeResult):
        all_data = {
            FEATURES: {key: flag.to_json_dict() for key, flag in merged.flags.items()},
            SEGMENTS: {key: segment.to_json_dict() for key, segment in merged.segments.items()},
        }
        try:
            self._sink_or_store().init(all_data)
            self._inited = True
            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)
        except Exception as e:
            log.error('Unable to store data: %s' % repr(e))
            traceback.print_exc()
            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(e)))

    def _handle_error(self, error: Exception):
        if self._data_source_update_sink is not None:
            self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, DataSourceErrorInfo(DataSourceErrorKind.INVALID_DATA, 0, time.time(), str(error)))

    def _start_auto_updater(self) -> Union[Poller, Watcher]:
        if have_watchdog and not self._force_polling:
            return Watcher(self._paths, self._reloader.trigger)
        poller = Poller(self._paths, self._poll_interval, self._reloader.trigger)
        poller.start()
        return poller
