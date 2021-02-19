import json
import pytest
from threading import Thread
import time
import uuid

from ldclient.config import Config, HTTPConfig
from ldclient.diagnostics import create_diagnostic_id, _DiagnosticAccumulator
from ldclient.event_processor import DefaultEventProcessor
from ldclient.util import log
from testing.http_util import start_server, BasicResponse
from testing.proxy_test_util import do_proxy_tests
from testing.stub_util import MockResponse, MockHttp


default_config = Config("fake_sdk_key")
user = {
    'key': 'userkey',
    'name': 'Red'
}
filtered_user = {
    'key': 'userkey',
    'privateAttrs': [ 'name' ]
}
numeric_user = {
    'key': 1,
    'secondary': 2,
    'ip': 3,
    'country': 4,
    'email': 5,
    'firstName': 6,
    'lastName': 7,
    'avatar': 8,
    'name': 9,
    'anonymous': False,
    'custom': {
        'age': 99
    }
}
stringified_numeric_user = {
    'key': '1',
    'secondary': '2',
    'ip': '3',
    'country': '4',
    'email': '5',
    'firstName': '6',
    'lastName': '7',
    'avatar': '8',
    'name': '9',
    'anonymous': False,
    'custom': {
        'age': 99
    }
}

ep = None
mock_http = None


def setup_function():
    global mock_http
    mock_http = MockHttp()

def teardown_function():
    if ep is not None:
        ep.stop()

class DefaultTestProcessor(DefaultEventProcessor):
    def __init__(self, **kwargs):
        if not 'diagnostic_opt_out' in kwargs:
            kwargs['diagnostic_opt_out'] = True
        if not 'sdk_key' in kwargs:
            kwargs['sdk_key'] = 'SDK_KEY'
        config = Config(**kwargs)
        diagnostic_accumulator = _DiagnosticAccumulator(create_diagnostic_id(config))
        DefaultEventProcessor.__init__(self, config, mock_http, diagnostic_accumulator = diagnostic_accumulator)

def test_identify_event_is_queued():
    with DefaultTestProcessor() as ep:
        e = { 'kind': 'identify', 'user': user }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        assert output == [{
            'kind': 'identify',
            'creationDate': e['creationDate'],
            'key': user['key'],
            'user': user
        }]

def test_user_is_filtered_in_identify_event():
    with DefaultTestProcessor(all_attributes_private = True) as ep:
        e = { 'kind': 'identify', 'user': user }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        assert output == [{
            'kind': 'identify',
            'creationDate': e['creationDate'],
            'key': user['key'],
            'user': filtered_user
        }]

def test_user_attrs_are_stringified_in_identify_event():
    with DefaultTestProcessor() as ep:
        e = { 'kind': 'identify', 'user': numeric_user }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        assert output == [{
            'kind': 'identify',
            'creationDate': e['creationDate'],
            'key': stringified_numeric_user['key'],
            'user': stringified_numeric_user
        }]

def test_individual_feature_event_is_queued_with_index_event():
    with DefaultTestProcessor() as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, user)
        check_feature_event(output[1], e, False, None)
        check_summary_event(output[2])

def test_user_is_filtered_in_index_event():
    with DefaultTestProcessor(all_attributes_private = True) as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, filtered_user)
        check_feature_event(output[1], e, False, None)
        check_summary_event(output[2])

def test_user_attrs_are_stringified_in_index_event():
    with DefaultTestProcessor() as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': numeric_user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, stringified_numeric_user)
        check_feature_event(output[1], e, False, None)
        check_summary_event(output[2])

def test_feature_event_can_contain_inline_user():
    with DefaultTestProcessor(inline_users_in_events = True) as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_feature_event(output[0], e, False, user)
        check_summary_event(output[1])

def test_user_is_filtered_in_feature_event():
    with DefaultTestProcessor(inline_users_in_events = True, all_attributes_private = True) as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_feature_event(output[0], e, False, filtered_user)
        check_summary_event(output[1])

def test_user_attrs_are_stringified_in_feature_event():
    with DefaultTestProcessor(inline_users_in_events = True) as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': numeric_user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_feature_event(output[0], e, False, stringified_numeric_user)
        check_summary_event(output[1])

def test_index_event_is_still_generated_if_inline_users_is_true_but_feature_event_is_not_tracked():
    with DefaultTestProcessor(inline_users_in_events = True) as ep:
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': False
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e, user)
        check_summary_event(output[1])

def test_two_events_for_same_user_only_produce_one_index_event():
    with DefaultTestProcessor(user_keys_flush_interval = 300) as ep:
        e0 = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        e1 = e0.copy()
        ep.send_event(e0)
        ep.send_event(e1)

        output = flush_and_get_events(ep)
        assert len(output) == 4
        check_index_event(output[0], e0, user)
        check_feature_event(output[1], e0, False, None)
        check_feature_event(output[2], e1, False, None)
        check_summary_event(output[3])

def test_new_index_event_is_added_if_user_cache_has_been_cleared():
    with DefaultTestProcessor(user_keys_flush_interval = 0.1) as ep:
        e0 = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        e1 = e0.copy()
        ep.send_event(e0)
        time.sleep(0.2)
        ep.send_event(e1)

        output = flush_and_get_events(ep)
        assert len(output) == 5
        check_index_event(output[0], e0, user)
        check_feature_event(output[1], e0, False, None)
        check_index_event(output[2], e1, user)
        check_feature_event(output[3], e1, False, None)
        check_summary_event(output[4])

def test_event_kind_is_debug_if_flag_is_temporarily_in_debug_mode():
    with DefaultTestProcessor() as ep:
        future_time = now() + 100000
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default',
            'trackEvents': False, 'debugEventsUntilDate': future_time
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, user)
        check_feature_event(output[1], e, True, user)
        check_summary_event(output[2])

def test_event_can_be_both_tracked_and_debugged():
    with DefaultTestProcessor() as ep:
        future_time = now() + 100000
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default',
            'trackEvents': True, 'debugEventsUntilDate': future_time
        }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 4
        check_index_event(output[0], e, user)
        check_feature_event(output[1], e, False, None)
        check_feature_event(output[2], e, True, user)
        check_summary_event(output[3])

def test_debug_mode_does_not_expire_if_both_client_time_and_server_time_are_before_expiration_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that slightly different from client time
        server_time = now() + 1000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event({ 'kind': 'identify', 'user': { 'key': 'otherUser' }})
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than both the client time and the server time
        debug_until = server_time + 10000
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default',
            'trackEvents': False, 'debugEventsUntilDate': debug_until
        }
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, user)
        check_feature_event(output[1], e, True, user)  # debug event
        check_summary_event(output[2])

def test_debug_mode_expires_based_on_client_time_if_client_time_is_later_than_server_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that is somewhat behind the client time
        server_time = now() - 20000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event({ 'kind': 'identify', 'user': { 'key': 'otherUser' }})
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than the server time, but in the past compared to the client.
        debug_until = server_time + 1000
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default',
            'trackEvents': False, 'debugEventsUntilDate': debug_until
        }
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e, user)
        check_summary_event(output[1])

def test_debug_mode_expires_based_on_server_time_if_server_time_is_later_than_client_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that is somewhat ahead of the client time
        server_time = now() + 20000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event({ 'kind': 'identify', 'user': { 'key': 'otherUser' }})
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than the client time, but in the past compared to the server.
        debug_until = server_time - 1000
        e = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default',
            'trackEvents': False, 'debugEventsUntilDate': debug_until
        }
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e, user)
        check_summary_event(output[1])

def test_two_feature_events_for_same_user_generate_only_one_index_event():
    with DefaultTestProcessor() as ep:
        e1 = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value1', 'default': 'default', 'trackEvents': False
        }
        e2 = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 2, 'value': 'value2', 'default': 'default', 'trackEvents': False
        }
        ep.send_event(e1)
        ep.send_event(e2)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e1, user)
        check_summary_event(output[1])

def test_nontracked_events_are_summarized():
    with DefaultTestProcessor() as ep:
        e1 = {
            'kind': 'feature', 'key': 'flagkey1', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value1', 'default': 'default1', 'trackEvents': False
        }
        e2 = {
            'kind': 'feature', 'key': 'flagkey2', 'version': 22, 'user': user,
            'variation': 2, 'value': 'value2', 'default': 'default2', 'trackEvents': False
        }
        ep.send_event(e1)
        ep.send_event(e2)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e1, user)
        se = output[1]
        assert se['kind'] == 'summary'
        assert se['startDate'] == e1['creationDate']
        assert se['endDate'] == e2['creationDate']
        assert se['features'] == {
            'flagkey1': {
                'default': 'default1',
                'counters': [ { 'version': 11, 'variation': 1, 'value': 'value1', 'count': 1 } ]
            },
            'flagkey2': {
                'default': 'default2',
                'counters': [ { 'version': 22, 'variation': 2, 'value': 'value2', 'count': 1 } ]
            }
        }

def test_custom_event_is_queued_with_user():
    with DefaultTestProcessor() as ep:
        e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '}, 'metricValue': 1.5 }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e, user)
        check_custom_event(output[1], e, None)

def test_custom_event_can_contain_inline_user():
    with DefaultTestProcessor(inline_users_in_events = True) as ep:
        e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '} }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_custom_event(output[0], e, user)

def test_user_is_filtered_in_custom_event():
    with DefaultTestProcessor(inline_users_in_events = True, all_attributes_private = True) as ep:
        e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '} }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_custom_event(output[0], e, filtered_user)

def test_user_attrs_are_stringified_in_custom_event():
    with DefaultTestProcessor(inline_users_in_events = True) as ep:
        e = { 'kind': 'custom', 'key': 'eventkey', 'user': numeric_user, 'data': { 'thing': 'stuff '} }
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_custom_event(output[0], e, stringified_numeric_user)

def test_nothing_is_sent_if_there_are_no_events():
    with DefaultTestProcessor() as ep:
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is None

def test_sdk_key_is_sent():
    with DefaultTestProcessor(sdk_key = 'SDK_KEY') as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('Authorization') == 'SDK_KEY'

def test_wrapper_header_not_sent_when_not_set():
    with DefaultTestProcessor() as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') is None

def test_wrapper_header_sent_when_set():
    with DefaultTestProcessor(wrapper_name = "Flask", wrapper_version = "0.0.1") as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') == "Flask/0.0.1"

def test_wrapper_header_sent_without_version():
    with DefaultTestProcessor(wrapper_name = "Flask") as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') == "Flask"

def test_event_schema_set_on_event_send():
    with DefaultTestProcessor() as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') == "3"

def test_sdk_key_is_sent_on_diagnostic_request():
    with DefaultTestProcessor(sdk_key = 'SDK_KEY', diagnostic_opt_out=False) as ep:
        ep._wait_until_inactive()
        assert mock_http.request_headers.get('Authorization') == 'SDK_KEY'

def test_event_schema_not_set_on_diagnostic_send():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        ep._wait_until_inactive()
        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') is None

def test_init_diagnostic_event_sent():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        diag_init = flush_and_get_events(ep)
        # Fields are tested in test_diagnostics.py
        assert len(diag_init) == 6
        assert diag_init['kind'] == 'diagnostic-init'

def test_periodic_diagnostic_includes_events_in_batch():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        # Ignore init event
        flush_and_get_events(ep)
        # Send a payload with a single event
        ep.send_event({ 'kind': 'identify', 'user': user })
        flush_and_get_events(ep)

        ep._send_diagnostic()
        diag_event = flush_and_get_events(ep)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 1
        assert diag_event['deduplicatedUsers'] == 0

def test_periodic_diagnostic_includes_deduplicated_users():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        # Ignore init event
        flush_and_get_events(ep)
        # Send two eval events with the same user to cause a user deduplication
        e0 = {
            'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
            'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
        }
        e1 = e0.copy();
        ep.send_event(e0)
        ep.send_event(e1)
        flush_and_get_events(ep)

        ep._send_diagnostic()
        diag_event = flush_and_get_events(ep)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 3
        assert diag_event['deduplicatedUsers'] == 1

def test_no_more_payloads_are_sent_after_401_error():
    verify_unrecoverable_http_error(401)

def test_no_more_payloads_are_sent_after_403_error():
    verify_unrecoverable_http_error(403)

def test_will_still_send_after_408_error():
    verify_recoverable_http_error(408)

def test_will_still_send_after_429_error():
    verify_recoverable_http_error(429)

def test_will_still_send_after_500_error():
    verify_recoverable_http_error(500)

def test_does_not_block_on_full_inbox():
    config = Config("fake_sdk_key", events_max_pending=1)  # this sets the size of both the inbox and the outbox to 1
    ep_inbox_holder = [ None ]
    ep_inbox = None

    def dispatcher_factory(inbox, config, http, diag):
        ep_inbox_holder[0] = inbox  # it's an array because otherwise it's hard for a closure to modify a variable
        return None  # the dispatcher object itself doesn't matter, we only manipulate the inbox
    def event_consumer():
        while True:
            message = ep_inbox.get(block=True)
            if message.type == 'stop':
                message.param.set()
                return
    def start_consuming_events():
        Thread(target=event_consumer).start()

    with DefaultEventProcessor(config, mock_http, dispatcher_factory) as ep:
        ep_inbox = ep_inbox_holder[0]
        event1 = { 'kind': 'custom', 'key': 'event1', 'user': user }
        event2 = { 'kind': 'custom', 'key': 'event2', 'user': user }
        ep.send_event(event1)
        ep.send_event(event2)  # this event should be dropped - inbox is full
        message1 = ep_inbox.get(block=False)
        had_no_more = ep_inbox.empty()
        start_consuming_events()
        assert message1.param == event1
        assert had_no_more

def test_http_proxy(monkeypatch):
    def _event_processor_proxy_test(server, config, secure):
        with DefaultEventProcessor(config) as ep:
            ep.send_event({ 'kind': 'identify', 'user': user })
            ep.flush()
            ep._wait_until_inactive()
    do_proxy_tests(_event_processor_proxy_test, 'POST', monkeypatch)

def verify_unrecoverable_http_error(status):
    with DefaultTestProcessor(sdk_key = 'SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is None

def verify_recoverable_http_error(status):
    with DefaultTestProcessor(sdk_key = 'SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is not None

def test_event_payload_id_is_sent():
    with DefaultEventProcessor(Config(sdk_key = 'SDK_KEY'), mock_http) as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        headerVal = mock_http.request_headers.get('X-LaunchDarkly-Payload-ID')
        assert headerVal is not None
        # Throws on invalid UUID
        uuid.UUID(headerVal)

def test_event_payload_id_changes_between_requests():
    with DefaultEventProcessor(Config(sdk_key = 'SDK_KEY'), mock_http) as ep:
        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        ep.send_event({ 'kind': 'identify', 'user': user })
        ep.flush()
        ep._wait_until_inactive()

        firstPayloadId = mock_http.recorded_requests[0][0].get('X-LaunchDarkly-Payload-ID')
        secondPayloadId = mock_http.recorded_requests[1][0].get('X-LaunchDarkly-Payload-ID')
        assert firstPayloadId != secondPayloadId

def flush_and_get_events(ep):
    ep.flush()
    ep._wait_until_inactive()
    if mock_http.request_data is None:
        raise AssertionError('Expected to get an HTTP request but did not get one')
    else:
        return json.loads(mock_http.request_data)

def check_index_event(data, source, user):
    assert data['kind'] == 'index'
    assert data['creationDate'] == source['creationDate']
    assert data['user'] == user

def check_feature_event(data, source, debug, inline_user):
    assert data['kind'] == ('debug' if debug else 'feature')
    assert data['creationDate'] == source['creationDate']
    assert data['key'] == source['key']
    assert data.get('version') == source.get('version')
    assert data.get('variation') == source.get('variation')
    assert data.get('value') == source.get('value')
    assert data.get('default') == source.get('default')
    if inline_user is None:
        assert data['userKey'] == str(source['user']['key'])
    else:
        assert data['user'] == inline_user

def check_custom_event(data, source, inline_user):
    assert data['kind'] == 'custom'
    assert data['creationDate'] == source['creationDate']
    assert data['key'] == source['key']
    assert data['data'] == source['data']
    if inline_user is None:
        assert data['userKey'] == source['user']['key']
    else:
        assert data['user'] == inline_user
    assert data.get('metricValue') == source.get('metricValue')

def check_summary_event(data):
    assert data['kind'] == 'summary'

def now():
    return int(time.time() * 1000)
