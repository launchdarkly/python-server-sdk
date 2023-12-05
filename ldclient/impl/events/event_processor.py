"""
Implementation details of the analytics event delivery component.
"""

from calendar import timegm
from collections import namedtuple
from email.utils import parsedate
import json
from threading import Event, Lock, Thread
from typing import Any, Optional
import time
import uuid
import queue
import urllib3
from random import Random

from ldclient.context import Context
from ldclient.impl.events.debug_event import DebugEvent
from ldclient.impl.events.diagnostics import create_diagnostic_init
from ldclient.impl.events.event_buffer import EventBuffer
from ldclient.impl.events.event_output_formatter import EventOutputFormatter
from ldclient.impl.events.index_event import IndexEvent
from ldclient.impl.events.types import EventInput, EventInputCustom, EventInputEvaluation, EventInputIdentify
from ldclient.migrations.tracker import MigrationOpEvent
from ldclient.impl.fixed_thread_pool import FixedThreadPool
from ldclient.impl.http import _http_factory
from ldclient.impl.lru_cache import SimpleLRUCache
from ldclient.impl.repeating_task import RepeatingTask
from ldclient.impl.util import check_if_error_is_recoverable_and_log, current_time_millis, is_http_error_recoverable, log, _headers
from ldclient.impl.sampler import Sampler
from ldclient.interfaces import EventProcessor

__MAX_FLUSH_THREADS__ = 5
__CURRENT_EVENT_SCHEMA__ = 4


EventProcessorMessage = namedtuple('EventProcessorMessage', ['type', 'param'])


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
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)

    def _do_send(self, output_events):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(output_events, separators=(',',':'))
            log.debug('Sending events payload: ' + json_body)
            payload_id = str(uuid.uuid4())
            r = _post_events_with_retry(
                self._http,
                self._config,
                self._config.events_uri,
                payload_id,
                json_body,
                "%d events" % len(self._payload.events)
            )
            if r:
                self._response_fn(r)
            return r
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed. [%s]', e)


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
            _post_events_with_retry(
                self._http,
                self._config,
                self._config.events_base_uri + '/diagnostic',
                None,
                json_body,
                "diagnostic event"
            )
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)

class EventDispatcher:
    def __init__(self, inbox, config, http_client, diagnostic_accumulator=None):
        self._inbox = inbox
        self._config = config
        self._http = _http_factory(config).create_pool_manager(1, config.events_uri) if http_client is None else http_client
        self._close_http = (http_client is None)  # so we know whether to close it later
        self._disabled = False
        self._outbox = EventBuffer(config.events_max_pending)
        self._context_keys = SimpleLRUCache(config.context_keys_capacity)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0
        self._deduplicated_contexts = 0
        self._diagnostic_accumulator = None if config.diagnostic_opt_out else diagnostic_accumulator
        self._sampler = Sampler(Random())

        self._flush_workers = FixedThreadPool(__MAX_FLUSH_THREADS__, "ldclient.flush")
        self._diagnostic_flush_workers = None if self._diagnostic_accumulator is None else FixedThreadPool(1, "ldclient.diag_flush")
        if self._diagnostic_accumulator is not None:
            init_event = create_diagnostic_init(self._diagnostic_accumulator.data_since_date,
                                                self._diagnostic_accumulator.diagnostic_id,
                                                config)
            task = DiagnosticEventSendTask(self._http, self._config, init_event)
            self._diagnostic_flush_workers.execute(task.run)

        self._main_thread = Thread(target=self._run_main_loop)
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

    def _process_event(self, event: EventInput):
        if self._disabled:
            return

        # Decide whether to add the event to the payload. Feature events may be added twice, once for
        # the event (if tracked) and once for debugging.
        context = None  # type: Optional[Context]
        can_add_index = True
        full_event = None  # type: Any
        debug_event = None  # type: Optional[DebugEvent]
        sampling_ratio = 1 if event.sampling_ratio is None else event.sampling_ratio

        if isinstance(event, EventInputEvaluation):
            context = event.context
            if not event.exclude_from_summaries:
                self._outbox.add_to_summary(event)
            if event.track_events:
                full_event = event
            if self._should_debug_event(event):
                debug_event = DebugEvent(event)
        elif isinstance(event, EventInputIdentify):
            context = event.context
            full_event = event
            can_add_index = False  # an index event would be redundant if there's an identify event
        elif isinstance(event, EventInputCustom):
            context = event.context
            full_event = event
        elif isinstance(event, MigrationOpEvent):
            full_event = event

        # For each context we haven't seen before, we add an index event - unless this is already
        # an identify event.
        if context is not None:
            already_seen = self._context_keys.put(context.fully_qualified_key, True)
            if can_add_index:
                if already_seen:
                    self._deduplicated_contexts += 1
                else:
                    self._outbox.add_event(IndexEvent(event.timestamp, context))

        if full_event and self._sampler.sample(sampling_ratio):
            self._outbox.add_event(full_event)

        if debug_event and self._sampler.sample(sampling_ratio):
            self._outbox.add_event(debug_event)

    def _should_debug_event(self, event: EventInputEvaluation):
        if event.flag is None:
            return False
        debug_until = event.flag.debug_events_until_date
        if debug_until is not None:
            last_past = self._last_known_past_time
            if debug_until > last_past and debug_until > current_time_millis():
                return True
        return False

    def _trigger_flush(self):
        if self._disabled:
            return
        payload = self._outbox.get_payload()
        if self._diagnostic_accumulator:
            self._diagnostic_accumulator.record_events_in_batch(len(payload.events))
        if len(payload.events) > 0 or not payload.summary.is_empty():
            task = EventPayloadSendTask(self._http, self._config, self._formatter, payload,
                self._handle_response)
            if self._flush_workers.execute(task.run):
                # The events have been handed off to a flush worker; clear them from our buffer.
                self._outbox.clear()
            else:
                # We're already at our limit of concurrent flushes; leave the events in the buffer.
                pass

    def _handle_response(self, r):
        server_date_str = r.headers.get('Date')
        if server_date_str is not None:
            server_date = parsedate(server_date_str)
            if server_date is not None:
                timestamp = int(timegm(server_date) * 1000)
                self._last_known_past_time = timestamp
        if r.status > 299 and not is_http_error_recoverable(r.status):
            self._disabled = True
            return

    def _send_and_reset_diagnostics(self):
        if self._diagnostic_accumulator is not None:
            dropped_event_count = self._outbox.get_and_clear_dropped_count()
            stats_event = self._diagnostic_accumulator.create_event_and_reset(dropped_event_count, self._deduplicated_contexts)
            self._deduplicated_contexts = 0
            task = DiagnosticEventSendTask(self._http, self._config, stats_event)
            self._diagnostic_flush_workers.execute(task.run)

    def _do_shutdown(self):
        self._flush_workers.stop()
        self._flush_workers.wait()
        if self._close_http:
            self._http.clear()


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config, http=None, dispatcher_class=None, diagnostic_accumulator=None):
        self._inbox = queue.Queue(config.events_max_pending)
        self._inbox_full = False
        self._flush_timer = RepeatingTask(config.flush_interval, config.flush_interval, self.flush)
        self._contexts_flush_timer = RepeatingTask(config.context_keys_flush_interval, config.context_keys_flush_interval, self._flush_contexts)
        self._flush_timer.start()
        self._contexts_flush_timer.start()
        if diagnostic_accumulator is not None:
            self._diagnostic_event_timer = RepeatingTask(config.diagnostic_recording_interval,
                config.diagnostic_recording_interval, self._send_diagnostic)
            self._diagnostic_event_timer.start()
        else:
            self._diagnostic_event_timer = None

        self._close_lock = Lock()
        self._closed = False

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
        # is full; an orderly shutdown can't happen unless these messages are received.
        self._post_message_and_wait('stop')

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

    def _post_message_and_wait(self, type):
        reply = Event()
        self._inbox.put(EventProcessorMessage(type, reply))
        reply.wait()

    # These magic methods allow use of the "with" block in tests
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop()


def _post_events_with_retry(
    http_client,
    config,
    uri,
    payload_id,
    body,
    events_description
):
    hdrs = _headers(config)
    hdrs['Content-Type'] = 'application/json'
    if payload_id:
        hdrs['X-LaunchDarkly-Event-Schema'] = str(__CURRENT_EVENT_SCHEMA__)
        hdrs['X-LaunchDarkly-Payload-ID'] = payload_id
    can_retry = True
    context = "posting %s" % events_description
    while True:
        next_action_message = "will retry" if can_retry else "some events were dropped"
        try:
            r = http_client.request(
                'POST',
                uri,
                headers=hdrs,
                body=body,
                timeout=urllib3.Timeout(connect=config.http.connect_timeout, read=config.http.read_timeout),
                retries=0
            )
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
