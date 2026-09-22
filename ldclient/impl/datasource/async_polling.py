"""
Default implementation of the polling component.
"""

# currently excluded from documentation - see docs/README.md

import time
from typing import Optional

from ldclient.async_config import AsyncConfig
from ldclient.impl.aio.concurrency import AsyncEvent, AsyncRepeatingTask
from ldclient.impl.datasource.datasource_common import sink_or_store
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
    AsyncFeatureRequester,
    AsyncFeatureStore,
    AsyncUpdateProcessor,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState
)


class AsyncPollingUpdateProcessor(AsyncUpdateProcessor):
    """Polls LaunchDarkly for flag data on its own background task.

    The loop reads its wait from the retry state, which ``_fetch_and_store``
    updates, so a failure can push the next poll further out than the poll
    interval. See :mod:`ldclient.impl.retry`.
    """

    def __init__(self, config: AsyncConfig, requester: AsyncFeatureRequester, store: AsyncFeatureStore, ready: AsyncEvent, retry_state: Optional[RetryState] = None):
        self._config = config
        self._data_source_update_sink = config.data_source_update_sink
        self._requester = requester
        self._store = store
        self._ready = ready
        self._retry = retry_state or for_polling(config.poll_interval)
        # No initial delay: the first poll is immediate.
        self._task = AsyncRepeatingTask("ldclient.datasource.polling", self._retry, 0, self._fetch_and_store)

    def start(self):
        log.info("Starting AsyncPollingUpdateProcessor with request interval: " + str(self._config.poll_interval))
        self._task.start()

    def initialized(self):
        return self._ready.is_set() and self._store.initialized

    async def stop(self):
        log.info("Stopping AsyncPollingUpdateProcessor")
        self._task.stop()

        if self._data_source_update_sink is not None:
            self._data_source_update_sink.update_status(DataSourceState.OFF, None)

        # OFF is reported first, so a listener sees the shutdown at once. The wait
        # that follows only drains a poll already in flight, so the transport is
        # not closed while that request still uses it. The close is in a finally,
        # so an owned transport is released even if stop() is cancelled mid-wait.
        try:
            await self._task.wait_stopped()
        finally:
            await self._requester.close()

    async def _fetch_and_store(self) -> None:
        """Makes one poll request and records the outcome on the retry state."""
        try:
            all_data = await self._requester.get_all_data()
            await sink_or_store(self._data_source_update_sink, self._store).init(all_data)

            if self._data_source_update_sink is not None:
                self._data_source_update_sink.update_status(DataSourceState.VALID, None)

            # Report the status before signaling readiness, so a caller that
            # wakes on readiness cannot still read INITIALIZING.
            if not self._ready.is_set() and self._store.initialized:
                log.info("AsyncPollingUpdateProcessor initialized ok")
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
