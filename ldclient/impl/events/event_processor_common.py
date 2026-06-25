"""
Shared data types and pure-transform helpers for the analytics event processors.

These classes contain no I/O and are used by both the sync (event_processor.py)
and async (async_event_processor.py) implementations.
"""

from collections import namedtuple
from typing import Any, Dict, List

from ldclient.config import Config
from ldclient.context import Context
from ldclient.impl.events.event_context_formatter import EventContextFormatter
from ldclient.impl.events.event_summarizer import EventSummarizer, EventSummary
from ldclient.impl.events.types import (
    EventInputCustom,
    EventInputEvaluation,
    EventInputIdentify
)
from ldclient.impl.util import log, timedelta_millis
from ldclient.migrations.tracker import MigrationOpEvent

# ---------------------------------------------------------------------------
# Shared event wrapper types
# ---------------------------------------------------------------------------


class DebugEvent:
    __slots__ = ['original_input']

    def __init__(self, original_input: EventInputEvaluation):
        self.original_input = original_input


class IndexEvent:
    __slots__ = ['timestamp', 'context']

    def __init__(self, timestamp: int, context: Context):
        self.timestamp = timestamp
        self.context = context


FlushPayload = namedtuple('FlushPayload', ['events', 'summary'])


# ---------------------------------------------------------------------------
# EventBuffer — in-memory accumulation buffer (no I/O)
# ---------------------------------------------------------------------------

class EventBuffer:
    def __init__(self, capacity: int):
        self._capacity = capacity
        self._events: List[Any] = []
        self._summarizer = EventSummarizer()
        self._exceeded_capacity = False
        self._dropped_events = 0

    def add_event(self, event: Any):
        if len(self._events) >= self._capacity:
            self._dropped_events += 1
            if not self._exceeded_capacity:
                log.warning("Exceeded event queue capacity. Increase capacity to avoid dropping events.")
                self._exceeded_capacity = True
        else:
            self._events.append(event)
            self._exceeded_capacity = False

    def add_to_summary(self, event: EventInputEvaluation):
        self._summarizer.summarize_event(event)

    def get_and_clear_dropped_count(self) -> int:
        count = self._dropped_events
        self._dropped_events = 0
        return count

    def get_payload(self) -> FlushPayload:
        return FlushPayload(self._events, self._summarizer.snapshot())

    def clear(self):
        self._events = []
        self._summarizer.clear()


# ---------------------------------------------------------------------------
# EventOutputFormatter — pure data transform (no I/O)
# ---------------------------------------------------------------------------

class EventOutputFormatter:
    def __init__(self, config: Config):
        self._context_formatter = EventContextFormatter(
            config.all_attributes_private, config.private_attributes
        )

    def make_output_events(self, events: List[Any], summary: EventSummary):
        events_out = [self.make_output_event(e) for e in events]
        if not summary.is_empty():
            events_out.append(self.make_summary_event(summary))
        return events_out

    def make_output_event(self, e: Any):
        if isinstance(e, EventInputEvaluation):
            out = self._base_eval_props(e, 'feature')
            out['context'] = self._process_context(e.context, True)
            return out
        elif isinstance(e, DebugEvent):
            out = self._base_eval_props(e.original_input, 'debug')
            out['context'] = self._process_context(e.original_input.context, False)
            return out
        elif isinstance(e, EventInputIdentify):
            return {
                'kind': 'identify',
                'creationDate': e.timestamp,
                'context': self._process_context(e.context, False),
            }
        elif isinstance(e, IndexEvent):
            return {
                'kind': 'index',
                'creationDate': e.timestamp,
                'context': self._process_context(e.context, False),
            }
        elif isinstance(e, EventInputCustom):
            out = {
                'kind': 'custom',
                'creationDate': e.timestamp,
                'key': e.key,
                'context': self._process_context(e.context, True),
            }
            if e.data is not None:
                out['data'] = e.data
            if e.metric_value is not None:
                out['metricValue'] = e.metric_value
            return out
        elif isinstance(e, MigrationOpEvent):
            out = {
                'kind': 'migration_op',
                'creationDate': e.timestamp,
                'operation': e.operation.value,
                'context': self._process_context(e.context, True),
                'evaluation': {'key': e.key, 'value': e.detail.value},
            }
            if e.flag is not None:
                out["evaluation"]["version"] = e.flag.version
            if e.default_stage:
                out["evaluation"]["default"] = e.default_stage.value
            if e.detail.variation_index is not None:
                out["evaluation"]["variation"] = e.detail.variation_index
            if e.detail.reason is not None:
                out["evaluation"]["reason"] = e.detail.reason
            if e.sampling_ratio is not None and e.sampling_ratio != 1:
                out["samplingRatio"] = e.sampling_ratio

            measurements: List[Dict] = []
            if len(e.invoked) > 0:
                measurements.append({"key": "invoked", "values": {o.value: True for o in e.invoked}})
            if e.consistent is not None:
                measurement = {"key": "consistent", "value": e.consistent}
                if e.consistent_ratio is not None and e.consistent_ratio != 1:
                    measurement["samplingRatio"] = e.consistent_ratio
                measurements.append(measurement)
            if len(e.latencies) > 0:
                measurements.append({"key": "latency_ms", "values": {o.value: timedelta_millis(d) for o, d in e.latencies.items()}})
            if len(e.errors) > 0:
                measurements.append({"key": "error", "values": {o.value: True for o in e.errors}})
            if measurements:
                out["measurements"] = measurements
            return out
        return None

    def make_summary_event(self, summary: EventSummary):
        """Transform summarizer data into the format used for the event payload."""
        flags_out: Dict[str, Any] = {}
        for key, flag_data in summary.flags.items():
            flag_data_out = {
                'default': flag_data.default,
                'contextKinds': list(flag_data.context_kinds),
            }
            counters = []
            for ckey, cval in flag_data.counters.items():
                variation, version = ckey
                counter = {'count': cval.count, 'value': cval.value}
                if variation is not None:
                    counter['variation'] = variation
                if version is None:
                    counter['unknown'] = True
                else:
                    counter['version'] = version
                counters.append(counter)
            flag_data_out['counters'] = counters
            flags_out[key] = flag_data_out
        return {
            'kind': 'summary',
            'startDate': summary.start_date,
            'endDate': summary.end_date,
            'features': flags_out,
        }

    def _process_context(self, context: Context, redact_anonymous: bool):
        if redact_anonymous:
            return self._context_formatter.format_context_redact_anonymous(context)
        return self._context_formatter.format_context(context)

    def _context_keys(self, context: Context):
        out = {}
        for i in range(context.individual_context_count):
            c = context.get_individual_context(i)
            if c is not None:
                out[c.kind] = c.key
        return out

    def _base_eval_props(self, e: EventInputEvaluation, kind: str) -> dict:
        out = {
            'kind': kind,
            'creationDate': e.timestamp,
            'key': e.key,
            'value': e.value,
            'default': e.default_value,
        }
        if e.flag is not None:
            out['version'] = e.flag.version
        if e.variation is not None:
            out['variation'] = e.variation
        if e.reason is not None:
            out['reason'] = e.reason
        if e.prereq_of is not None:
            out['prereqOf'] = e.prereq_of.key
        return out
