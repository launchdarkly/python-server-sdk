"""
Tests for how analytics events treat override-affected evaluations: they appear in summary
counters only, under a counter that carries the override-affected marker, and they produce no
individual feature event and no debug event.
"""
import json
import time
from typing import Any, Dict, List

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.config import Config
from ldclient.context import Context
from ldclient.impl.events.async_event_processor import (
    DefaultAsyncEventProcessor
)
from ldclient.impl.events.event_processor import DefaultEventProcessor
from ldclient.impl.events.event_processor_common import EventOutputFormatter
from ldclient.impl.events.event_summarizer import (
    EventSummarizer,
    EventSummaryCounter
)
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.impl.events.test_async_event_processor import MockAioHttp
from ldclient.testing.stub_util import MockHttp

context = Context.builder('userkey').name('Red').build()
timestamp = 10000


def tracked_flag(key: str = 'flagkey', version: int = 2):
    """A flag that requests individual feature events and debug events far into the future."""
    return FlagBuilder(key).version(version).track_events(True).debug_events_until_date(int(time.time() * 1000) + 100000).build()


def evaluation(flag, value: str = 'value', variation: int = 1, override_affected: bool = False, track_events: bool = True) -> EventInputEvaluation:
    return EventInputEvaluation(timestamp, context, flag.key, flag, variation, value, None, 'default', None, track_events, override_affected)


def events_of_kind(output: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    return [e for e in output if e['kind'] == kind]


# ---------------------------------------------------------------------------
# Summarizer and output formatter
# ---------------------------------------------------------------------------

def test_summarizer_keeps_override_affected_counters_separate():
    flag = tracked_flag()
    es = EventSummarizer()
    es.summarize_event(evaluation(flag))
    es.summarize_event(evaluation(flag, override_affected=True))
    es.summarize_event(evaluation(flag, override_affected=True))
    es.summarize_event(evaluation(flag))
    counters = es.snapshot().flags[flag.key].counters
    assert counters == {
        (1, flag.version, False): EventSummaryCounter(2, 'value'),
        (1, flag.version, True): EventSummaryCounter(2, 'value'),
    }


def test_summary_output_carries_the_marker_only_on_override_affected_counters():
    flag = tracked_flag()
    es = EventSummarizer()
    es.summarize_event(evaluation(flag))
    es.summarize_event(evaluation(flag, override_affected=True))
    es.summarize_event(EventInputEvaluation(timestamp, context, 'unknown-flag', None, None, 'default', None, 'default', None, False, False))
    output = EventOutputFormatter(Config('SDK_KEY')).make_summary_event(es.snapshot())

    counters = output['features'][flag.key]['counters']
    assert len(counters) == 2
    by_marker = {c.get('overrideAffected'): c for c in counters}
    assert by_marker[None] == {'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version}
    assert by_marker[True] == {'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version, 'overrideAffected': True}
    assert 'overrideAffected' not in json.dumps(output['features']['unknown-flag'])


def test_event_input_evaluation_defaults_to_not_override_affected():
    event = EventInputEvaluation(timestamp, context, 'flag', None, None, 'default', None, 'default')
    assert event.override_affected is False
    assert event.to_debugging_dict()['override_affected'] is False


# ---------------------------------------------------------------------------
# Sync event processor
# ---------------------------------------------------------------------------

def flush_and_get_events(ep: DefaultEventProcessor, mock_http: MockHttp) -> List[Dict[str, Any]]:
    ep.flush()
    ep._wait_until_inactive()
    assert mock_http.request_data is not None, 'Expected to get an HTTP request but did not get one'
    return json.loads(mock_http.request_data)


def make_processor(mock_http: MockHttp) -> DefaultEventProcessor:
    return DefaultEventProcessor(Config('SDK_KEY', diagnostic_opt_out=True), mock_http)


def test_override_affected_evaluation_produces_no_feature_or_debug_event():
    flag = tracked_flag()
    mock_http = MockHttp()
    with make_processor(mock_http) as ep:
        ep.send_event(evaluation(flag, override_affected=True))
        output = flush_and_get_events(ep, mock_http)

    assert [e['kind'] for e in output] == ['index', 'summary']
    counters = output[1]['features'][flag.key]['counters']
    assert counters == [{'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version, 'overrideAffected': True}]


def test_ordinary_evaluation_still_produces_feature_and_debug_events():
    flag = tracked_flag()
    mock_http = MockHttp()
    with make_processor(mock_http) as ep:
        ep.send_event(evaluation(flag))
        output = flush_and_get_events(ep, mock_http)

    assert sorted(e['kind'] for e in output) == ['debug', 'feature', 'index', 'summary']
    counters = events_of_kind(output, 'summary')[0]['features'][flag.key]['counters']
    assert counters == [{'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version}]


def test_mixed_evaluations_of_one_flag_split_into_two_counters_and_one_feature_event():
    flag = tracked_flag()
    mock_http = MockHttp()
    with make_processor(mock_http) as ep:
        ep.send_event(evaluation(flag, override_affected=True))
        ep.send_event(evaluation(flag, override_affected=True))
        ep.send_event(evaluation(flag))
        output = flush_and_get_events(ep, mock_http)

    assert len(events_of_kind(output, 'feature')) == 1
    assert len(events_of_kind(output, 'debug')) == 1
    counters = events_of_kind(output, 'summary')[0]['features'][flag.key]['counters']
    assert sorted(counters, key=lambda c: c['count']) == [
        {'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version},
        {'count': 2, 'value': 'value', 'variation': 1, 'version': flag.version, 'overrideAffected': True},
    ]


def test_override_affected_prerequisite_record_produces_no_feature_event():
    parent = tracked_flag('parent', 1)
    prereq = tracked_flag('prereq', 7)
    mock_http = MockHttp()
    with make_processor(mock_http) as ep:
        ep.send_event(EventInputEvaluation(timestamp, context, prereq.key, prereq, 1, 'value', None, None, parent, True, True))
        output = flush_and_get_events(ep, mock_http)

    assert [e['kind'] for e in output] == ['index', 'summary']
    counters = output[1]['features'][prereq.key]['counters']
    assert counters == [{'count': 1, 'value': 'value', 'variation': 1, 'version': prereq.version, 'overrideAffected': True}]


def test_override_affected_evaluation_still_produces_an_index_event():
    flag = tracked_flag()
    mock_http = MockHttp()
    with make_processor(mock_http) as ep:
        ep.send_event(evaluation(flag, override_affected=True))
        output = flush_and_get_events(ep, mock_http)
    assert output[0]['kind'] == 'index'
    assert output[0]['context'] == context.to_dict()


# ---------------------------------------------------------------------------
# Async event processor (shares the dispatch logic)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_processor_suppresses_individual_events_for_override_affected_evaluations():
    flag = tracked_flag()
    mock_http = MockAioHttp()
    ep = DefaultAsyncEventProcessor(AsyncConfig('SDK_KEY', diagnostic_opt_out=True), mock_http)
    try:
        ep.send_event(evaluation(flag, override_affected=True))
        ep.send_event(evaluation(flag))
        assert await ep.flush_and_wait(5) is True
        output = json.loads(mock_http.request_data)
    finally:
        await ep.stop()

    assert sorted(e['kind'] for e in output) == ['debug', 'feature', 'index', 'summary']
    counters = events_of_kind(output, 'summary')[0]['features'][flag.key]['counters']
    assert sorted(counters, key=lambda c: 'overrideAffected' in c) == [
        {'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version},
        {'count': 1, 'value': 'value', 'variation': 1, 'version': flag.version, 'overrideAffected': True},
    ]
