import pytest

from ldclient.event_summarizer import EventSummarizer


user = { 'key': 'user1' }

def test_summarize_event_does_nothing_for_identify_event():
	es = EventSummarizer()
	snapshot = es.snapshot()
	es.summarize_event({ 'kind': 'identify', 'creationDate': 1000, 'user': user })

	assert es.snapshot() == snapshot

def test_summarize_event_does_nothing_for_custom_event():
	es = EventSummarizer()
	snapshot = es.snapshot()
	es.summarize_event({ 'kind': 'custom', 'creationDate': 1000, 'key': 'eventkey', 'user': user })

	assert es.snapshot() == snapshot

def test_summarize_event_sets_start_and_end_dates():
	es = EventSummarizer()
	event1 = { 'kind': 'feature', 'creationDate': 2000, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	event2 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	event3 = { 'kind': 'feature', 'creationDate': 1500, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	es.summarize_event(event1)
	es.summarize_event(event2)
	es.summarize_event(event3)
	data = es.snapshot()

	assert data.start_date == 1000
	assert data.end_date == 2000

def test_summarize_event_increments_counters():
	es = EventSummarizer()
	event1 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag1', 'user': user,
		'version': 11, 'variation': 1, 'value': 'value1', 'default': 'default1' }
	event2 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag1', 'user': user,
		'version': 11, 'variation': 2, 'value': 'value2', 'default': 'default1' }
	event3 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag2', 'user': user,
		'version': 22, 'variation': 1, 'value': 'value99', 'default': 'default2' }
	event4 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag1', 'user': user,
		'version': 11, 'variation': 1, 'value': 'value1', 'default': 'default1' }
	event5 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'badkey', 'user': user,
		'version': None, 'variation': None, 'value': 'default3', 'default': 'default3' }
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
