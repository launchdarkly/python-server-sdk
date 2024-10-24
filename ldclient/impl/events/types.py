import json
from typing import Any, Callable, Optional

from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl import AnyNum
from ldclient.impl.model import FeatureFlag
from ldclient.impl.util import current_time_millis

# These event types are not the event data that is sent to LaunchDarkly; they're the input
# parameters that are passed to EventProcessor, which translates them into event data (for
# instance, many evaluations may produce just one summary event). Since the SDK generates
# these at high volume, we want them to be efficient so we use attributes and slots rather
# than dictionaries.


class EventInput:
    __slots__ = ['timestamp', 'context', 'sampling_ratio']

    def __init__(self, timestamp: int, context: Context, sampling_ratio: Optional[int] = None):
        self.timestamp = timestamp
        self.context = context
        self.sampling_ratio = sampling_ratio

    def __repr__(self) -> str:  # used only in test debugging
        return "%s(%s)" % (self.__class__.__name__, json.dumps(self.to_debugging_dict()))

    def __eq__(self, other) -> bool:  # used only in tests
        return isinstance(other, EventInput) and self.to_debugging_dict() == other.to_debugging_dict()

    def to_debugging_dict(self) -> dict:
        return {}


class EventInputEvaluation(EventInput):
    __slots__ = ['key', 'flag', 'variation', 'value', 'reason', 'default_value', 'prereq_of', 'track_events', 'sampling_ratio', 'exclude_from_summaries']

    def __init__(
        self,
        timestamp: int,
        context: Context,
        key: str,
        flag: Optional[FeatureFlag],
        variation: Optional[int],
        value: Any,
        reason: Optional[dict],
        default_value: Any,
        prereq_of: Optional[FeatureFlag] = None,
        track_events: bool = False,
    ):
        super().__init__(timestamp, context, 1 if flag is None else flag.sampling_ratio)
        self.key = key
        self.flag = flag
        self.variation = variation
        self.value = value
        self.reason = reason
        self.default_value = default_value
        self.prereq_of = prereq_of
        self.track_events = track_events
        self.exclude_from_summaries = False if flag is None else flag.exclude_from_summaries

    def to_debugging_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "context": self.context.to_dict(),
            "key": self.key,
            "flag": {"key": self.flag.key} if self.flag else None,
            "variation": self.variation,
            "value": self.value,
            "reason": self.reason,
            "default_value": self.default_value,
            "prereq_of": {"key": self.prereq_of.key} if self.prereq_of else None,
            "track_events": self.track_events,
            "exclude_from_summaries": self.exclude_from_summaries,
            "sampling_ratio": self.sampling_ratio,
        }


class EventInputIdentify(EventInput):
    def to_debugging_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "context": self.context.to_dict(),
            "sampling_ratio": self.sampling_ratio,
        }


class EventInputCustom(EventInput):
    __slots__ = ['key', 'data', 'metric_value']

    def __init__(self, timestamp: int, context: Context, key: str, data: Any = None, metric_value: Optional[AnyNum] = None):
        super().__init__(timestamp, context)
        self.key = key
        self.data = data
        self.metric_value = metric_value  # type: Optional[int|float|complex]

    def to_debugging_dict(self) -> dict:
        return {"timestamp": self.timestamp, "context": self.context.to_dict(), "sampling_ratio": self.sampling_ratio, "key": self.key, "data": self.data, "metric_value": self.metric_value}


# Event constructors are centralized here to avoid mistakes and repetitive logic.
# The LDClient owns two instances of EventFactory: one that always embeds evaluation reasons
# in the events (for when variation_detail is called) and one that doesn't.
#
# Note that none of these methods fill in the "creationDate" property, because in the Python
# client, that is done by DefaultEventProcessor.send_event().


class EventFactory:
    def __init__(self, with_reasons: bool, timestamp_fn: Callable[[], int] = current_time_millis):
        self._with_reasons = with_reasons
        self._timestamp_fn = timestamp_fn

    def new_eval_event(self, flag: FeatureFlag, context: Context, detail: EvaluationDetail, default_value: Any, prereq_of_flag: Optional[FeatureFlag] = None) -> EventInputEvaluation:
        add_experiment_data = self.is_experiment(flag, detail.reason)
        return EventInputEvaluation(
            self._timestamp_fn(),
            context,
            flag.key,
            flag,
            detail.variation_index,
            detail.value,
            detail.reason if self._with_reasons or add_experiment_data else None,
            default_value,
            prereq_of_flag,
            flag.track_events or add_experiment_data,
        )

    def new_default_event(self, flag: FeatureFlag, context: Context, default_value: Any, reason: Optional[dict]) -> EventInputEvaluation:
        return EventInputEvaluation(self._timestamp_fn(), context, flag.key, flag, None, default_value, reason if self._with_reasons else None, default_value, None, flag.track_events)

    def new_unknown_flag_event(self, key: str, context: Context, default_value: Any, reason: Optional[dict]) -> EventInputEvaluation:
        return EventInputEvaluation(self._timestamp_fn(), context, key, None, None, default_value, reason if self._with_reasons else None, default_value, None, False)

    def new_identify_event(self, context: Context) -> EventInputIdentify:
        return EventInputIdentify(self._timestamp_fn(), context)

    def new_custom_event(self, event_name: str, context: Context, data: Any, metric_value: Optional[AnyNum]) -> EventInputCustom:
        return EventInputCustom(self._timestamp_fn(), context, event_name, data, metric_value)

    @staticmethod
    def is_experiment(flag: FeatureFlag, reason: Optional[dict]) -> bool:
        if reason is not None:
            if reason.get('inExperiment'):
                return True
            kind = reason['kind']
            if kind == 'RULE_MATCH':
                index = reason['ruleIndex']
                rules = flag.rules
                return index >= 0 and index < len(rules) and rules[index].track_events
            elif kind == 'FALLTHROUGH':
                return flag.track_events_fallthrough
        return False
