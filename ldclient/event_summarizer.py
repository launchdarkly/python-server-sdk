"""
Implementation details of the analytics event delivery component.
"""
# currently excluded from documentation - see docs/README.md

from collections import namedtuple


EventSummary = namedtuple('EventSummary', ['start_date', 'end_date', 'counters'])


class EventSummarizer:
    def __init__(self):
        self.start_date = 0
        self.end_date = 0
        self.counters = dict()

    """
    Add this event to our counters, if it is a type of event we need to count.
    """
    def summarize_event(self, event):
        if event['kind'] == 'feature':
            counter_key = (event['key'], event.get('variation'), event.get('version'))
            counter_val = self.counters.get(counter_key)
            if counter_val is None:
                counter_val = { 'count': 1, 'value': event['value'], 'default': event.get('default') }
                self.counters[counter_key] = counter_val
            else:
                counter_val['count'] = counter_val['count'] + 1
            date = event['creationDate']
            if self.start_date == 0 or date < self.start_date:
                self.start_date = date
            if date > self.end_date:
                self.end_date = date

    """
    Return the current summarized event data.
    """
    def snapshot(self):
        return EventSummary(start_date = self.start_date, end_date = self.end_date, counters = self.counters)

    def clear(self):
        self.start_date = 0
        self.end_date = 0
        self.counters = dict()
