"""
Implementation details of the analytics event delivery component.
"""

import gzip
import json
import queue
import time
import uuid
from collections import namedtuple
from random import Random
from threading import Event, Lock, Thread
from typing import Optional

import urllib3

from ldclient.config import Config
from ldclient.impl.events.diagnostics import create_diagnostic_init
from ldclient.impl.events.event_processor_common import (
    CURRENT_EVENT_SCHEMA,
    EventBuffer,
    EventDispatcherBase,
    EventOutputFormatter
)
from ldclient.impl.events.types import EventInput
from ldclient.impl.fixed_thread_pool import FixedThreadPool
from ldclient.impl.http import _http_factory
from ldclient.impl.lru_cache import SimpleLRUCache
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.sampler import Sampler
from ldclient.impl.util import (
    _headers,
    check_if_error_is_recoverable_and_log,
    log
)
from ldclient.interfaces import EventProcessor

__MAX_FLUSH_THREADS__ = 5


EventProcessorMessage = namedtuple('EventProcessorMessage', ['type', 'param'])


class _Deadline:
    """
    Tracks how much of a time budget is left, so that a sequence of blocking waits can share a
    single overall limit. A timeout of None means there is no limit.
    """

    def __init__(self, timeout: Optional[float]):
        self._end = None if timeout is None else time.monotonic() + timeout

    def remaining(self) -> Optional[float]:
        if self._end is None:
            return None
        return max(0.0, self._end - time.monotonic())


class EventPayloadSendTask:
    def __init__(self, http, config, formatter, payload, response_fn):
        self._http = http
        self._config = config
        self._formatter = formatter
        self._payload = payload
        self._response_fn = response_fn

    def run(self):
        try:
            output_events = self._formatter.make_output_events(self._payload.events, self._payload.summary)
            resp = self._do_send(output_events)
        except Exception as e:
            log.warning('Unhandled exception in event processor. Analytics events were not processed.', exc_info=True)

    def _do_send(self, output_events):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(output_events, separators=(',', ':'))
            log.debug('Sending events payload: ' + json_body)
            payload_id = str(uuid.uuid4())
            r = _post_events_with_retry(self._http, self._config, self._config.events_uri, payload_id, json_body, "%d events" % len(output_events))
            if r:
                self._response_fn(r)
            return r
        except Exception as e:
            log.warning('Unhandled exception in event processor. Analytics events were not processed. [%s]', e)


class DiagnosticEventSendTask:
    def __init__(self, http, config, event_body):
        self._http = http
        self._config = config
        self._event_body = event_body

    def run(self):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(self._event_body)
            log.debug('Sending diagnostic event: ' + json_body)
            _post_events_with_retry(self._http, self._config, self._config.events_base_uri + '/diagnostic', None, json_body, "diagnostic event")
        except Exception as e:
            log.warning('Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)


class EventDispatcher(EventDispatcherBase):
    def __init__(self, inbox, config, http_client, diagnostic_accumulator=None):
        self._inbox = inbox
        self._config = config
        self._http = _http_factory(config).create_pool_manager(1, config.events_uri) if http_client is None else http_client
        self._close_http = http_client is None  # so we know whether to close it later
        self._disabled = False
        self._outbox = EventBuffer(config.events_max_pending)
        self._context_keys = SimpleLRUCache(config.context_keys_capacity)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0
        self._deduplicated_contexts = 0
        self._diagnostic_accumulator = None if config.diagnostic_opt_out else diagnostic_accumulator
        self._sampler = Sampler(Random())
        self._omit_anonymous_contexts = config.omit_anonymous_contexts

        self._flush_workers = FixedThreadPool(__MAX_FLUSH_THREADS__, "ldclient.flush")
        self._diagnostic_flush_workers = None if self._diagnostic_accumulator is None else FixedThreadPool(1, "ldclient.events.diag_flush")
        if self._diagnostic_accumulator is not None:
            init_event = create_diagnostic_init(self._diagnostic_accumulator.data_since_date, self._diagnostic_accumulator.diagnostic_id, config)
            task = DiagnosticEventSendTask(self._http, self._config, init_event)
            self._diagnostic_flush_workers.execute(task.run)

        self._main_thread = Thread(target=self._run_main_loop, name="ldclient.events.processor")
        self._main_thread.daemon = True
        self._main_thread.start()

    def _run_main_loop(self):
        log.info("Starting event processor")
        while True:
            try:
                message = self._inbox.get(block=True)
                if message.type == 'event':
                    self._process_event(message.param)
                elif message.type == 'flush':
                    self._trigger_flush()
                elif message.type == 'flush_contexts':
                    self._context_keys.clear()
                elif message.type == 'diagnostic':
                    self._send_and_reset_diagnostics()
                elif message.type == 'test_sync':
                    self._flush_workers.wait()
                    if self._diagnostic_accumulator is not None:
                        self._diagnostic_flush_workers.wait()
                    message.param.set()
                elif message.type == 'stop':
                    self._do_shutdown()
                    message.param.set()
                    return
            except Exception as e:
                log.error('Unhandled exception in event processor', exc_info=True)

    def _trigger_flush(self):
        if self._disabled:
            return
        payload = self._outbox.get_payload()
        if self._diagnostic_accumulator:
            self._diagnostic_accumulator.record_events_in_batch(len(payload.events))
        if len(payload.events) > 0 or not payload.summary.is_empty():
            task = EventPayloadSendTask(self._http, self._config, self._formatter, payload, self._handle_response)
            if self._flush_workers.execute(task.run):
                # The events have been handed off to a flush worker; clear them from our buffer.
                self._outbox.clear()
            else:
                # We're already at our limit of concurrent flushes; leave the events in the buffer.
                pass

    def _send_and_reset_diagnostics(self):
        if self._diagnostic_accumulator is not None:
            dropped_event_count = self._outbox.get_and_clear_dropped_count()
            stats_event = self._diagnostic_accumulator.create_event_and_reset(dropped_event_count, self._deduplicated_contexts)
            self._deduplicated_contexts = 0
            task = DiagnosticEventSendTask(self._http, self._config, stats_event)
            self._diagnostic_flush_workers.execute(task.run)

    def _do_shutdown(self):
        # Delivery of an event payload can block for an unbounded time - notably when name
        # resolution hangs, which the connect and read timeouts do not cover - so these waits are
        # bounded. Worker threads are daemons, so any that are still stuck do not keep the process
        # alive; the events they were carrying are simply lost.
        deadline = _Deadline(self._config.shutdown_timeout)

        self._flush_workers.stop()
        drained = self._flush_workers.wait(deadline.remaining())

        if self._diagnostic_flush_workers:
            self._diagnostic_flush_workers.stop()
            drained = self._diagnostic_flush_workers.wait(deadline.remaining()) and drained

        if not drained:
            log.warning("Timed out waiting for analytics events to be delivered while shutting down; some events were dropped")

        if self._close_http:
            self._http.clear()


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config, http=None, dispatcher_class=None, diagnostic_accumulator=None):
        self._inbox = queue.Queue(config.events_max_pending)
        self._inbox_full = False
        self._flush_timer = RepeatingTask("ldclient.events.flush", config.flush_interval, config.flush_interval, self.flush)
        self._contexts_flush_timer = RepeatingTask("ldclient.events.context-flush", config.context_keys_flush_interval, config.context_keys_flush_interval, self._flush_contexts)
        self._flush_timer.start()
        self._contexts_flush_timer.start()
        if diagnostic_accumulator is not None:
            self._diagnostic_event_timer = RepeatingTask("ldclient.events.send-diagnostic", config.diagnostic_recording_interval, config.diagnostic_recording_interval, self._send_diagnostic)
            self._diagnostic_event_timer.start()
        else:
            self._diagnostic_event_timer = None

        self._close_lock = Lock()
        self._closed = False
        self._shutdown_timeout = config.shutdown_timeout

        (dispatcher_class or EventDispatcher)(self._inbox, config, http, diagnostic_accumulator)

    def send_event(self, event: EventInput):
        self._post_to_inbox(EventProcessorMessage('event', event))

    def flush(self):
        self._post_to_inbox(EventProcessorMessage('flush', None))

    def stop(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self._flush_timer.stop()
        self._contexts_flush_timer.stop()
        if self._diagnostic_event_timer:
            self._diagnostic_event_timer.stop()
        self.flush()
        # Note that here we are not calling _post_to_inbox, because we *do* want to wait if the inbox
        # is full; an orderly shutdown can't happen unless these messages are received. The wait is
        # bounded, though, so that a stalled event delivery cannot block the caller forever.
        if not self._post_message_and_wait('stop', self._shutdown_timeout):
            log.warning("Timed out waiting for the event processor to shut down after %s seconds; some analytics events may not have been delivered" % self._shutdown_timeout)

    def _post_to_inbox(self, message):
        try:
            self._inbox.put(message, block=False)
        except queue.Full:
            if not self._inbox_full:
                # possible race condition here, but it's of no real consequence - we'd just get an extra log line
                self._inbox_full = True
                log.warning("Events are being produced faster than they can be processed; some events will be dropped")

    def _flush_contexts(self):
        self._inbox.put(EventProcessorMessage('flush_contexts', None))

    def _send_diagnostic(self):
        self._inbox.put(EventProcessorMessage('diagnostic', None))

    # Used only in tests
    def _wait_until_inactive(self):
        self._post_message_and_wait('test_sync')

    def _post_message_and_wait(self, type, timeout: Optional[float] = None) -> bool:
        """
        Posts a message to the dispatcher and waits for it to be handled, for at most the given
        number of seconds (None means wait indefinitely). Returns True if it was handled, or False
        if the timeout elapsed while posting the message or while waiting for the reply.
        """
        reply = Event()
        deadline = _Deadline(timeout)
        try:
            remaining = deadline.remaining()
            if remaining is None:
                self._inbox.put(EventProcessorMessage(type, reply))
            else:
                # A zero timeout means "don't block at all" to Queue.put, so treat it as such
                # rather than passing a value it would reject.
                self._inbox.put(EventProcessorMessage(type, reply), block=remaining > 0, timeout=remaining or None)
        except queue.Full:
            return False
        return reply.wait(deadline.remaining())

    # These magic methods allow use of the "with" block in tests
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


def _post_events_with_retry(http_client, config, uri, payload_id, body, events_description):
    hdrs = _headers(config)
    hdrs['Content-Type'] = 'application/json'
    if config.enable_event_compression:
        hdrs['Content-Encoding'] = 'gzip'

    if payload_id:
        hdrs['X-LaunchDarkly-Event-Schema'] = str(CURRENT_EVENT_SCHEMA)
        hdrs['X-LaunchDarkly-Payload-ID'] = payload_id
    can_retry = True
    context = "posting %s" % events_description
    data = gzip.compress(bytes(body, 'utf-8')) if config.enable_event_compression else body
    while True:
        next_action_message = "will retry" if can_retry else "some events were dropped"
        try:
            r = http_client.request('POST', uri, headers=hdrs, body=data, timeout=urllib3.Timeout(connect=config.http.connect_timeout, read=config.http.read_timeout), retries=0)
            if r.status < 300:
                return r
            recoverable = check_if_error_is_recoverable_and_log(context, r.status, None, next_action_message)
            if not recoverable:
                return r
        except Exception as e:
            check_if_error_is_recoverable_and_log(context, None, str(e), next_action_message)
        if not can_retry:
            return None
        can_retry = False
        # fixed delay of 1 second for event retries
        time.sleep(1)
