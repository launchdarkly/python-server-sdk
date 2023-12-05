from collections import namedtuple
from typing import Any

from ldclient import log
from ldclient.impl.events.event_summarizer import EventSummarizer
from ldclient.impl.events.types import EventInputEvaluation

FlushPayload = namedtuple('FlushPayload', ['events', 'summary'])

class EventBuffer:
    def __init__(self, capacity):
        self._capacity = capacity
        self._events = []
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

    def get_and_clear_dropped_count(self):
        dropped_count = self._dropped_events
        self._dropped_events = 0
        return dropped_count

    def get_payload(self):
        return FlushPayload(self._events, self._summarizer.snapshot())

    def clear(self):
        self._events = []
        self._summarizer.clear()
