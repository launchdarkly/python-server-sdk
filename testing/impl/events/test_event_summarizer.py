import pytest

from ldclient.impl.events.event_summarizer import EventSummarizer
from ldclient.impl.events.types import *

from testing.builders import *


user = { 'key': 'user1' }
flag1 = FlagBuilder('flag1').version(11).build()
flag2 = FlagBuilder('flag2').version(22).build()


def test_summarize_event_sets_start_and_end_dates():
	es = EventSummarizer()
	event1 = EventInputEvaluation(2000, user, flag1.key, flag1, 0, '', None, None)
	event2 = EventInputEvaluation(1000, user, flag1.key, flag1, 0, '', None, None)
	event3 = EventInputEvaluation(1500, user, flag1.key, flag1, 0, '', None, None)
	es.summarize_event(event1)
	es.summarize_event(event2)
	es.summarize_event(event3)
	data = es.snapshot()

	assert data.start_date == 1000
	assert data.end_date == 2000

def test_summarize_event_increments_counters():
	es = EventSummarizer()
	event1 = EventInputEvaluation(1000, user, flag1.key, flag1, 1, 'value1', None, 'default1')
	event2 = EventInputEvaluation(1000, user, flag1.key, flag1, 2, 'value2', None, 'default1')
	event3 = EventInputEvaluation(1000, user, flag2.key, flag2, 1, 'value99', None, 'default2')
	event4 = EventInputEvaluation(1000, user, flag1.key, flag1, 1, 'value1', None, 'default1')
	event5 = EventInputEvaluation(1000, user, 'badkey', None, None, 'default3', None, 'default3')
	es.summarize_event(event1)
	es.summarize_event(event2)
	es.summarize_event(event3)
	es.summarize_event(event4)
	es.summarize_event(event5)
	data = es.snapshot()

	expected = {
		('flag1', 1, 11): { 'count': 2, 'value': 'value1', 'default': 'default1' },
		('flag1', 2, 11): { 'count': 1, 'value': 'value2', 'default': 'default1' },
		('flag2', 1, 22): { 'count': 1, 'value': 'value99', 'default': 'default2' },
		('badkey', None, None): { 'count': 1, 'value': 'default3', 'default': 'default3' }
	}
	assert data.counters == expected
