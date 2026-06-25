"""
Implementation details of the analytics event delivery component.
"""

import asyncio
import gzip
import json
import queue
import time
import uuid
from collections import namedtuple
from random import Random
from typing import Callable, Optional, Union

from ldclient.async_config import AsyncConfig
from ldclient.impl.aio.concurrency import (
    AsyncEvent,
    AsyncLock,
    AsyncQueue,
    AsyncRepeatingTask,
    AsyncTaskRunner,
    AsyncWorkerPool
)
from ldclient.impl.aio.transport import AsyncHTTPTransport
from ldclient.impl.events.diagnostics import create_diagnostic_init
from ldclient.impl.events.event_processor_common import (
    EventBuffer,
    EventDispatcherBase,
    EventOutputFormatter
)
from ldclient.impl.events.types import EventInput
from ldclient.impl.lru_cache import SimpleLRUCache
from ldclient.impl.sampler import Sampler
from ldclient.impl.util import (
    _headers,
    check_if_error_is_recoverable_and_log,
    log
)
from ldclient.interfaces import EventProcessor

__MAX_FLUSH_THREADS__ = 5
__CURRENT_EVENT_SCHEMA__ = 4


EventProcessorMessage = namedtuple('EventProcessorMessage', ['type', 'param'])


class EventPayloadSendTask:
    def __init__(self, http: AsyncHTTPTransport, config: AsyncConfig, formatter: EventOutputFormatter, payload, response_fn: Callable):
        self._http = http
        self._config = config
        self._formatter = formatter
        self._payload = payload
        self._response_fn = response_fn

    async def run(self):
        try:
            output_events = self._formatter.make_output_events(self._payload.events, self._payload.summary)
            await self._do_send(output_events)
        except Exception:
            log.warning('Unhandled exception in event processor. Analytics events were not processed.', exc_info=True)

    async def _do_send(self, output_events):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(output_events, separators=(',', ':'))
            log.debug('Sending events payload: ' + json_body)
            payload_id = str(uuid.uuid4())
            r = await _post_events_with_retry(self._http, self._config, self._config.events_uri, payload_id, json_body, "%d events" % len(output_events))
            if r:
                self._response_fn(r)
            return r
        except Exception as e:
            log.warning('Unhandled exception in event processor. Analytics events were not processed. [%s]', e)


class DiagnosticEventSendTask:
    def __init__(self, http: AsyncHTTPTransport, config: AsyncConfig, event_body: dict):
        self._http = http
        self._config = config
        self._event_body = event_body

    async def run(self):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(self._event_body)
            log.debug('Sending diagnostic event: ' + json_body)
            await _post_events_with_retry(self._http, self._config, self._config.events_base_uri + '/diagnostic', None, json_body, "diagnostic event")
        except Exception as e:
            log.warning('Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)


class EventDispatcher(EventDispatcherBase):
    def __init__(self, inbox: AsyncQueue, config: AsyncConfig, http_client, diagnostic_accumulator=None):
        self._inbox = inbox
        self._config = config
        # When no client is injected, the transport creates one targeting the
        # events URI and owns it (closing it on shutdown); an injected client
        # remains owned by the caller.
        self._http = AsyncHTTPTransport(config, client=http_client)
        self._disabled = False
        self._outbox = EventBuffer(config.events_max_pending)
        self._context_keys = SimpleLRUCache(config.context_keys_capacity)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0
        self._deduplicated_contexts = 0
        self._diagnostic_accumulator = None if config.diagnostic_opt_out else diagnostic_accumulator
        self._sampler = Sampler(Random())
        self._omit_anonymous_contexts = config.omit_anonymous_contexts

        self._flush_workers = AsyncWorkerPool(__MAX_FLUSH_THREADS__, "ldclient.flush")
        self._diagnostic_flush_workers: Optional[AsyncWorkerPool] = None
        if self._diagnostic_accumulator is not None:
            self._diagnostic_flush_workers = AsyncWorkerPool(1, "ldclient.events.diag_flush")
            init_event = create_diagnostic_init(self._diagnostic_accumulator.data_since_date, self._diagnostic_accumulator.diagnostic_id, config)
            task = DiagnosticEventSendTask(self._http, self._config, init_event)
            self._diagnostic_flush_workers.execute(task.run)

        self._runner = AsyncTaskRunner()
        self._runner.spawn("ldclient.events.processor", self._run_main_loop)

    async def _run_main_loop(self):
        log.info("Starting event processor")
        while True:
            try:
                message = await self._inbox.get()
                if message.type == 'event':
                    self._process_event(message.param)
                elif message.type == 'flush':
                    self._trigger_flush()
                elif message.type == 'flush_contexts':
                    self._context_keys.clear()
                elif message.type == 'diagnostic':
                    self._send_and_reset_diagnostics()
                elif message.type == 'test_sync':
                    await self._flush_workers.wait()
                    if self._diagnostic_flush_workers is not None:
                        await self._diagnostic_flush_workers.wait()
                    message.param.set()
                elif message.type == 'stop':
                    await self._do_shutdown()
                    message.param.set()
                    return
            except Exception:
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
        if self._diagnostic_accumulator is not None and self._diagnostic_flush_workers is not None:
            dropped_event_count = self._outbox.get_and_clear_dropped_count()
            stats_event = self._diagnostic_accumulator.create_event_and_reset(dropped_event_count, self._deduplicated_contexts)
            self._deduplicated_contexts = 0
            task = DiagnosticEventSendTask(self._http, self._config, stats_event)
            self._diagnostic_flush_workers.execute(task.run)

    async def _do_shutdown(self):
        self._flush_workers.stop()
        await self._flush_workers.wait()

        if self._diagnostic_flush_workers is not None:
            self._diagnostic_flush_workers.stop()
            await self._diagnostic_flush_workers.wait()

        await self._http.close()


class AsyncEventProcessor(EventProcessor):
    def __init__(self, config: AsyncConfig, http=None, dispatcher_class=None, diagnostic_accumulator=None):
        self._inbox = AsyncQueue(config.events_max_pending)
        self._inbox_full = False
        self._flush_timer = AsyncRepeatingTask("ldclient.events.flush", config.flush_interval, config.flush_interval, self.flush)
        self._contexts_flush_timer = AsyncRepeatingTask("ldclient.events.context-flush", config.context_keys_flush_interval, config.context_keys_flush_interval, self._flush_contexts)
        self._flush_timer.start()
        self._contexts_flush_timer.start()
        self._diagnostic_event_timer: Optional[AsyncRepeatingTask]
        if diagnostic_accumulator is not None:
            self._diagnostic_event_timer = AsyncRepeatingTask("ldclient.events.send-diagnostic", config.diagnostic_recording_interval, config.diagnostic_recording_interval, self._send_diagnostic)
            self._diagnostic_event_timer.start()
        else:
            self._diagnostic_event_timer = None

        self._close_lock = AsyncLock()
        self._closed = False

        (dispatcher_class or EventDispatcher)(self._inbox, config, http, diagnostic_accumulator)

    def send_event(self, event: EventInput):
        self._post_to_inbox(EventProcessorMessage('event', event))

    def flush(self):
        self._post_to_inbox(EventProcessorMessage('flush', None))

    async def stop(self):
        async with self._close_lock:
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
        await self._post_message_and_wait('stop')

    def _post_to_inbox(self, message: EventProcessorMessage):
        try:
            self._inbox.put_nowait(message)
        except queue.Full:
            if not self._inbox_full:
                # possible race condition here, but it's of no real consequence - we'd just get an extra log line
                self._inbox_full = True
                log.warning("Events are being produced faster than they can be processed; some events will be dropped")

    async def _flush_contexts(self):
        await self._inbox.put(EventProcessorMessage('flush_contexts', None))

    async def _send_diagnostic(self):
        await self._inbox.put(EventProcessorMessage('diagnostic', None))

    # Used only in tests
    async def _wait_until_inactive(self):
        await self._post_message_and_wait('test_sync')

    async def _post_message_and_wait(self, type):
        reply = AsyncEvent()
        await self._inbox.put(EventProcessorMessage(type, reply))
        await reply.wait()

    # These magic methods allow use of the "with" block in tests
    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        await self.stop()


async def _post_events_with_retry(http_client: AsyncHTTPTransport, config: AsyncConfig, uri: str, payload_id: Optional[str], body: str, events_description: str):
    hdrs = _headers(config)
    hdrs['Content-Type'] = 'application/json'
    if config.enable_event_compression:
        hdrs['Content-Encoding'] = 'gzip'

    if payload_id:
        hdrs['X-LaunchDarkly-Event-Schema'] = str(__CURRENT_EVENT_SCHEMA__)
        hdrs['X-LaunchDarkly-Payload-ID'] = payload_id
    can_retry = True
    context = "posting %s" % events_description
    data: Union[bytes, str] = gzip.compress(bytes(body, 'utf-8')) if config.enable_event_compression else body
    while True:
        next_action_message = "will retry" if can_retry else "some events were dropped"
        try:
            r = await http_client.request('POST', uri, headers=hdrs, body=data)
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
        await asyncio.sleep(1)
