"""
Implementation details of the analytics event delivery component.
"""

from collections import namedtuple
from typing import Any, Dict, List, Optional, Set, Tuple

from ldclient.impl.events.types import EventInputEvaluation


class EventSummaryCounter:
    __slots__ = ['count', 'value']

    def __init__(self, count: int, value: Any):
        self.count = count
        self.value = value

    def __eq__(self, other: Any) -> bool:  # used only in tests
        return isinstance(other, EventSummaryCounter) and other.count == self.count and other.value == self.value

    def __repr__(self) -> str:  # used only in test debugging
        return "EventSummaryCounter(%d, %s)" % (self.count, self.value)


class EventSummaryFlag:
    __slots__ = ['context_kinds', 'default', 'counters']

    def __init__(self, context_kinds: Set[str], default: Any, counters: Dict[Tuple[Optional[int], Optional[int]], EventSummaryCounter]):
        self.context_kinds = context_kinds
        self.counters = counters
        self.default = default

    def __eq__(self, other: Any) -> bool:  # used only in tests
        return isinstance(other, EventSummaryFlag) and other.context_kinds == self.context_kinds and other.counters == self.counters and other.default == self.default

    def __repr__(self) -> str:  # used only in test debugging
        return "EventSummaryFlag(%s, %s, %s)" % (self.context_kinds, self.counters, self.default)


class EventSummary:
    __slots__ = ['start_date', 'end_date', 'flags']

    def __init__(self, start_date: int, end_date: int, flags: Dict[str, EventSummaryFlag]):
        self.start_date = start_date
        self.end_date = end_date
        self.flags = flags

    def is_empty(self) -> bool:
        return len(self.flags) == 0


class EventSummarizer:
    def __init__(self):
        self.start_date = 0
        self.end_date = 0
        self.flags = dict()  # type: Dict[str, EventSummaryFlag]

    """
    Add this event to our counters, if it is a type of event we need to count.
    """

    def summarize_event(self, event: EventInputEvaluation):
        flag_data = self.flags.get(event.key)
        if flag_data is None:
            flag_data = EventSummaryFlag(set(), event.default_value, dict())
            self.flags[event.key] = flag_data

        context = event.context
        for i in range(context.individual_context_count):
            c = context.get_individual_context(i)
            if c is not None:
                flag_data.context_kinds.add(c.kind)

        counter_key = (event.variation, None if event.flag is None else event.flag.version)
        counter = flag_data.counters.get(counter_key)
        if counter is None:
            counter = EventSummaryCounter(1, event.value)
            flag_data.counters[counter_key] = counter
        else:
            counter.count += 1

        date = event.timestamp
        if self.start_date == 0 or date < self.start_date:
            self.start_date = date
        if date > self.end_date:
            self.end_date = date

    """
    Return the current summarized event data.
    """

    def snapshot(self):
        return EventSummary(start_date=self.start_date, end_date=self.end_date, flags=self.flags)

    def clear(self):
        self.start_date = 0
        self.end_date = 0
        self.flags = dict()
