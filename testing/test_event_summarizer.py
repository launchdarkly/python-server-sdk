import pytest

from ldclient.config import Config
from ldclient.event_summarizer import EventSummarizer


user = { 'key': 'user1' }

def test_notice_user_returns_false_for_never_seen_user():
	es = EventSummarizer(Config())
	assert es.notice_user(user) == False

def test_notice_user_returns_true_for_previously_seen_user():
	es = EventSummarizer(Config())
	es.notice_user(user)
	assert es.notice_user({ 'key': user['key'] }) == True

def test_oldest_user_forgotten_if_capacity_exceeded():
	es = EventSummarizer(Config(user_keys_capacity = 2))
	user1 = { 'key': 'user1' }
	user2 = { 'key': 'user2' }
	user3 = { 'key': 'user3' }
	es.notice_user(user1)
	es.notice_user(user2)
	es.notice_user(user3)
	assert es.notice_user(user3) == True
	assert es.notice_user(user2) == True
	assert es.notice_user(user1) == False

def test_summarize_event_does_nothing_for_identify_event():
	es = EventSummarizer(Config())
	snapshot = es.snapshot()
	es.summarize_event({ 'kind': 'identify', 'creationDate': 1000, 'user': user })

	assert es.snapshot() == snapshot

def test_summarize_event_does_nothing_for_custom_event():
	es = EventSummarizer(Config())
	snapshot = es.snapshot()
	es.summarize_event({ 'kind': 'custom', 'creationDate': 1000, 'key': 'eventkey', 'user': user })

	assert es.snapshot() == snapshot

def test_summarize_event_sets_start_and_end_dates():
	es = EventSummarizer(Config())
	event1 = { 'kind': 'feature', 'creationDate': 2000, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	event2 = { 'kind': 'feature', 'creationDate': 1000, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	event3 = { 'kind': 'feature', 'creationDate': 1500, 'key': 'flag', 'user': user,
		'version': 1, 'variation': 0, 'value': '', 'default': None }
	es.summarize_event(event1)
	es.summarize_event(event2)
	es.summarize_event(event3)
	data = es.output(es.snapshot())

	assert data['start_date'] == 1000
	assert data['end_date'] == 2000

def test_summarize_event_increments_counters():
	es = EventSummarizer(Config())
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
	data = es.output(es.snapshot())

	data['features']['flag1']['counters'].sort(key = lambda c: c['value'])
	expected = {
		'start_date': 1000,
		'end_date': 1000,
		'features': {
			'flag1': {
				'default': 'default1',
				'counters': [
					{ 'version': 11, 'value': 'value1', 'count': 2 },
					{ 'version': 11, 'value': 'value2', 'count': 1 }
				]
			},
			'flag2': {
				'default': 'default2',
				'counters': [
					{ 'version': 22, 'value': 'value99', 'count': 1 }
				]
			},
			'badkey': {
				'default': 'default3',
				'counters': [
					{ 'unknown': True, 'value': 'default3', 'count': 1}
				]
			}
		}
	}
	assert data == expected
