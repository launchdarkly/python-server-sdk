from typing import List, Any, Dict

from ldclient.context import Context
from ldclient.impl.events.debug_event import DebugEvent
from ldclient.impl.events.event_context_formatter import EventContextFormatter
from ldclient.impl.events.event_summarizer import EventSummary
from ldclient.impl.events.index_event import IndexEvent
from ldclient.impl.events.types import EventInputEvaluation, EventInputIdentify, EventInputCustom
from ldclient.impl.util import timedelta_millis
from ldclient.migrations.tracker import MigrationOpEvent


class EventOutputFormatter:
    def __init__(self, config):
        self._context_formatter = EventContextFormatter(config.all_attributes_private, config.private_attributes)

    def make_output_events(self, events: List[Any], summary: EventSummary):
        events_out = [ self.make_output_event(e) for e in events ]
        if not summary.is_empty():
            events_out.append(self.make_summary_event(summary))
        return events_out

    def make_output_event(self, e: Any):
        if isinstance(e, EventInputEvaluation):
            out = self._base_eval_props(e, 'feature')
            out['contextKeys'] = self._context_keys(e.context)
            return out
        elif isinstance(e, DebugEvent):
            out = self._base_eval_props(e.original_input, 'debug')
            out['context'] = self._process_context(e.original_input.context)
            return out
        elif isinstance(e, EventInputIdentify):
            return {
                'kind': 'identify',
                'creationDate': e.timestamp,
                'context': self._process_context(e.context)
            }
        elif isinstance(e, IndexEvent):
            return {
                'kind': 'index',
                'creationDate': e.timestamp,
                'context': self._process_context(e.context)
            }
        elif isinstance(e, EventInputCustom):
            out = {
                'kind': 'custom',
                'creationDate': e.timestamp,
                'key': e.key,
                'contextKeys': self._context_keys(e.context)
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
                'contextKeys': self._context_keys(e.context),
                'evaluation': {
                    'key': e.key,
                    'value': e.detail.value
                }
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
                measurements.append(
                    {
                        "key": "invoked",
                        "values": {origin.value: True for origin in e.invoked}
                    }
                )

            if e.consistent is not None:
                measurement = {
                    "key": "consistent",
                    "value": e.consistent
                }

                if e.consistent_ratio is not None and e.consistent_ratio != 1:
                    measurement["samplingRatio"] = e.consistent_ratio

                measurements.append(measurement)

            if len(e.latencies) > 0:
                measurements.append(
                    {
                        "key": "latency_ms",
                        "values": {o.value: timedelta_millis(d) for o, d in e.latencies.items()}
                    }
                )

            if len(e.errors) > 0:
                measurements.append(
                    {
                        "key": "error",
                        "values": {origin.value: True for origin in e.errors}
                    }
                )

            if len(measurements):
                out["measurements"] = measurements

            return out

        return None

    """
    Transform summarizer data into the format used for the event payload.
    """
    def make_summary_event(self, summary: EventSummary):
        flags_out = dict()  # type: dict[str, Any]
        for key, flag_data in summary.flags.items():
            flag_data_out = {'default': flag_data.default, 'contextKinds': list(flag_data.context_kinds)}
            counters = []  # type: list[dict[str, Any]]
            for ckey, cval in flag_data.counters.items():
                variation, version = ckey
                counter = {
                    'count': cval.count,
                    'value': cval.value
                }
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
            'features': flags_out
        }

    def _process_context(self, context: Context):
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
            'default': e.default_value
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
