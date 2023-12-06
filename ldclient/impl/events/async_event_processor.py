import asyncio
import json
import uuid
from email.utils import parsedate
from calendar import timegm
from random import Random
from typing import Optional

from ldclient.config import Config
from ldclient.impl.events.debug_event import DebugEvent
from ldclient.impl.events.diagnostics import create_diagnostic_init
from ldclient.impl.events.event_buffer import EventBuffer
from ldclient.impl.events.event_output_formatter import EventOutputFormatter
from ldclient.impl.events.index_event import IndexEvent
from ldclient.impl.events.types import EventInput, EventInputEvaluation, EventInputIdentify, EventInputCustom
from ldclient.impl.lru_cache import SimpleLRUCache
from ldclient.impl.sampler import Sampler
from ldclient.interfaces import EventProcessor
from ldclient.impl.util import log, _headers, is_http_error_recoverable, current_time_millis, \
    check_if_error_is_recoverable_and_log
from ldclient.migrations.tracker import MigrationOpEvent
from ldclient.impl.events.diagnostics import _DiagnosticAccumulator

import aiohttp

__CURRENT_EVENT_SCHEMA__ = 4


class AsyncDefaultEventProcessor(EventProcessor):

    def __init__(self, config: Config,
                 diagnostic_accumulator: Optional[_DiagnosticAccumulator] = None):
        self._event_buffer = EventBuffer(config.events_max_pending)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0
        self._deduplicated_contexts = 0
        self._diagnostic_accumulator = None if config.diagnostic_opt_out else diagnostic_accumulator
        self._publish_task = asyncio.create_task(self._event_publishing_loop(config.flush_interval))
        self._cache_clear_task = asyncio.create_task(
            self._context_keys_flush_loop(config.context_keys_flush_interval))
        self._diagnostic_task = asyncio.create_task(
            self._diagnostic_events_loop(config.diagnostic_recording_interval))
        self._config = config
        self._disabled = False
        self._sampler = Sampler(Random())
        self._context_key_lru_cache = SimpleLRUCache(config.context_keys_capacity)
        # TODO: Share the same client session with as much of the SDK as possible.
        self._http_client_session = aiohttp.ClientSession()

    async def _event_publishing_loop(self, flush_interval: int):
        while True:
            # TODO: Calculate the delay to account for time sending.
            await asyncio.sleep(flush_interval)
            await self._do_flush()

    async def _context_keys_flush_loop(self, flush_interval: int):
        while True:
            await asyncio.sleep(flush_interval)
            await self._context_key_lru_cache.clear()

    async def _send_and_reset_diagnostics(self):
        if self._diagnostic_accumulator is None:
            return
        try:
            dropped_event_count = self._outbox.get_and_clear_dropped_count()
            stats_event = self._diagnostic_accumulator.create_event_and_reset(dropped_event_count,
                                                                              self._deduplicated_contexts)
            self._deduplicated_contexts = 0
            json_body = json.dumps(stats_event)
            log.debug('Sending diagnostic event: ' + json_body)
            await self._post_events_with_retry(
                self._config.events_base_uri + '/diagnostic',
                json_body,
                "diagnostic event"
            )

        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)

    async def _diagnostic_events_loop(self, flush_interval: int):
        init_event = create_diagnostic_init(self._diagnostic_accumulator.data_since_date,
                                            self._diagnostic_accumulator.diagnostic_id,
                                            self._config)
        try:
            json_body = json.dumps(init_event)
            log.debug('Sending diagnostic event: ' + json_body)
            await self._post_events_with_retry(
                self._config.events_base_uri + '/diagnostic',
                json_body,
                "diagnostic event"
            )
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)
        while True:
            await asyncio.sleep(flush_interval)
            await self._send_and_reset_diagnostics()

    async def _do_flush(self):
        # noinspection PyBroadException
        try:
            payload = self._event_buffer.get_payload()
            self._event_buffer.clear()
            if len(payload.events) > 0 or not payload.summary.is_empty():
                output_events = self._formatter.make_output_events(payload.events, payload.summary)
                await self._do_send(output_events, len(payload.events))

        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)

    async def _do_send(self, payload, event_count):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(payload, separators=(',', ':'))
            log.debug('Sending events payload: ' + json_body)
            payload_id = str(uuid.uuid4())
            response = await self._post_events_with_retry(
                self._config.events_uri,
                payload_id,
                json_body,
                "%d events" % event_count
            )
            if response:
                self._handle_response(response)
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed. [%s]', e)

    def _handle_response(self, response):
        server_date_str = response.headers.get('Date')
        if server_date_str is not None:
            server_date = parsedate(server_date_str)
            if server_date is not None:
                timestamp = int(timegm(server_date) * 1000)
                self._last_known_past_time = timestamp
        if response.status > 299 and not is_http_error_recoverable(response.status):
            self._disabled = True
            return

    async def _post_events_with_retry(
            self,
            uri,
            payload_id,
            body,
            events_description
    ):
        headers = _headers(self._config)
        headers['Content-Type'] = 'application/json'
        if payload_id:
            headers['X-LaunchDarkly-Event-Schema'] = str(__CURRENT_EVENT_SCHEMA__)
            headers['X-LaunchDarkly-Payload-ID'] = payload_id
        can_retry = True
        context = "posting %s" % events_description
        while True:
            next_action_message = "will retry" if can_retry else "some events were dropped"
            try:
                response = await self._http_client_session.post(uri, data=body, headers=headers)
                response.close()

                if response.status < 300:
                    return response
                recoverable = check_if_error_is_recoverable_and_log(context, response.status, None, next_action_message)
                if not recoverable:
                    return response
            except Exception as e:
                check_if_error_is_recoverable_and_log(context, None, str(e), next_action_message)
            if not can_retry:
                return None
            can_retry = False
            # fixed delay of 1 second for event retries
            await asyncio.sleep(1)

    def _should_debug_event(self, event: EventInputEvaluation):
        if event.flag is None:
            return False
        debug_until = event.flag.debug_events_until_date
        if debug_until is not None:
            last_past = self._last_known_past_time
            if debug_until > last_past and debug_until > current_time_millis():
                return True
        return False

    def send_event(self, event):
        asyncio.create_task(self._send_event(event))

    async def _send_event(self, event: EventInput):
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
                self._event_buffer.add_to_summary(event)
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
            already_seen = self._context_key_lru_cache.put(context.fully_qualified_key, True)
            if can_add_index:
                if already_seen:
                    self._deduplicated_contexts += 1
                else:
                    self._event_buffer.add_event(IndexEvent(event.timestamp, context))

        if full_event and self._sampler.sample(sampling_ratio):
            self._event_buffer.add_event(full_event)

        if debug_event and self._sampler.sample(sampling_ratio):
            self._event_buffer.add_event(debug_event)

    def flush(self):
        asyncio.create_task(self._do_flush())

    def stop(self):
        asyncio.create_task(self._stop())

    async def _stop(self):
        self._publish_task.cancel()
        self._cache_clear_task.cancel()
        self._diagnostic_task.cancel()
        await self._http_client_session.close()
