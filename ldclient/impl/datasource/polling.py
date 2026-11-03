"""
Default implementation of the polling component.
"""

# currently excluded from documentation - see docs/README.md

import time
from threading import Event
from typing import Any, Mapping, Optional, Protocol, Tuple, runtime_checkable

from ldclient.config import Config
from ldclient.impl.datasource.datasource_common import (
    record_environment_id,
    sink_or_store
)
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.retry import (
    FailureKind,
    RetryState,
    classify_http_status,
    for_polling
)
from ldclient.impl.util import (
    UnsuccessfulResponseException,
    http_error_description,
    log
)
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceUpdateSink,
    FeatureRequester,
    FeatureStore,
    UpdateProcessor
)


@runtime_checkable
class _FeatureRequesterWithHeaders(Protocol):
    def get_all_data_with_headers(self) -> Tuple[Any, Optional[Mapping[str, str]]]:
        ...


class PollingUpdateProcessor(UpdateProcessor):
    """Polls LaunchDarkly for flag data on its own worker thread.

    The task reads its wait from the retry state, which ``_poll`` updates, so a
    failure can push the next poll further out than the poll interval. See
    :mod:`ldclient.impl.retry`.
    """

    def __init__(self, config: Config, requester: FeatureRequester, store: FeatureStore, ready: Event, retry_state: Optional[RetryState] = None):
        self._config = config
        self._data_source_update_sink: Optional[DataSourceUpdateSink] = config.data_source_update_sink
        self._requester = requester
        self._store = store
        self._ready = ready
        self._retry = retry_state or for_polling(config.poll_interval)
        self._task = RepeatingTask("ldclient.datasource.polling", self._retry, 0, self._poll)

    def start(self):
        log.info("Starting PollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        self._task.start()

    def initialized(self):
        return self._ready.is_set() is True and self._store.initialized is True

    def stop(self):
        log.info("Stopping PollingUpdateProcessor")
        self._task.stop()

        if self._data_source_update_sink is None:
            return

        self._data_source_update_sink.update_status(DataSourceState.OFF, None)

    def _poll(self) -> None:
        """Makes one poll request and records the outcome on the retry state."""
        try:
            (all_data, headers) = self._get_all_data_with_headers()
            record_environment_id(self._data_source_update_sink, headers)
            sink_or_store(self._data_source_update_sink, self._store).init(all_data)

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)

            # Report the status before signaling readiness, so a caller that
            # wakes on readiness cannot still read INITIALIZING.
            if not self._ready.is_set() and self._store.initialized:
                log.info("PollingUpdateProcessor initialized ok")
                self._ready.set()

            self._retry.record_success()
            return
        except UnsuccessfulResponseException as e:
            kind = classify_http_status(e.status)
            error_info = DataSourceErrorInfo(DataSourceErrorKind.ERROR_RESPONSE, e.status, time.time(), str(e))
            description = "Received %s for polling request" % http_error_description(e.status)
            level = log.error if kind is FailureKind.UNEXPECTED else log.warning
            stacktrace = None
        except Exception as e:
            kind = FailureKind.NORMAL
            error_info = DataSourceErrorInfo(DataSourceErrorKind.UNKNOWN, 0, time.time(), str(e))
            description = "Error encountered when updating flags: %s" % e
            level = log.error
            # The exception is passed explicitly: by the time the message is
            # logged, the handler has exited and exc_info() is empty.
            stacktrace = e

        self._retry.record_failure(kind)
        delay = self._retry.next_delay
        level("%s - will retry in %.1fs" % (description, delay), exc_info=stacktrace)

        if self._data_source_update_sink is not None:
            self._data_source_update_sink.update_status(DataSourceState.INTERRUPTED, error_info)

    def _get_all_data_with_headers(self) -> Tuple[Any, Optional[Mapping[str, str]]]:
        """
        Externally provided feature requesters are not required to surface
        response headers, so fall back to the data-only method.
        """
        if isinstance(self._requester, _FeatureRequesterWithHeaders):
            return self._requester.get_all_data_with_headers()

        return (self._requester.get_all_data(), None)
