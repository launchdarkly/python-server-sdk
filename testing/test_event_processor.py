from email.utils import formatdate
import json
import pytest
from requests.structures import CaseInsensitiveDict
import time

from ldclient.config import Config
from ldclient.event_processor import DefaultEventProcessor

from ldclient.util import log

default_config = Config()
user = {
    'key': 'userkey',
    'name': 'Red'
}
filtered_user = {
    'key': 'userkey',
    'privateAttrs': [ 'name' ]
}

ep = None
mock_session = None


class MockResponse(object):
    def __init__(self, status, headers):
        self._status = status
        self._headers = headers

    @property
    def status_code(self):
        return self._status

    @property
    def headers(self):
        return self._headers

    def raise_for_status(self):
        pass

class MockSession(object):
    def __init__(self):
        self._request_data = None
        self._request_headers = None
        self._response_status = 200
        self._server_time = None

    def post(self, uri, headers, timeout, data):
        self._request_headers = headers
        self._request_data = data
        resp_hdr = CaseInsensitiveDict()
        if self._server_time is not None:
            resp_hdr['Date'] = formatdate(self._server_time / 1000, localtime=False, usegmt=True)
        return MockResponse(self._response_status, resp_hdr)

    def close(self):
        pass

    @property
    def request_data(self):
        return self._request_data

    @property
    def request_headers(self):
        return self._request_headers

    def set_response_status(self, status):
        self._response_status = status
    
    def set_server_time(self, timestamp):
        self._server_time = timestamp

    def clear(self):
        self._request_headers = None
        self._request_data = None


def setup_function():
    global mock_session
    mock_session = MockSession()

def teardown_function():
    if ep is not None:
        ep.stop()

def setup_processor(config):
    global ep
    ep = DefaultEventProcessor(config, mock_session)
    ep.start()


def test_identify_event_is_queued():
    setup_processor(Config())

    e = { 'kind': 'identify', 'user': user }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 1
    assert output == [{
        'kind': 'identify',
        'creationDate': e['creationDate'],
        'user': user
    }]

def test_user_is_filtered_in_identify_event():
    setup_processor(Config(all_attributes_private = True))

    e = { 'kind': 'identify', 'user': user }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 1
    assert output == [{
        'kind': 'identify',
        'creationDate': e['creationDate'],
        'user': filtered_user
    }]

def test_individual_feature_event_is_queued_with_index_event():
    setup_processor(Config())

    e = {
        'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
        'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
    }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 3
    check_index_event(output[0], e, user)
    check_feature_event(output[1], e, False, None)
    check_summary_event(output[2])

def test_user_is_filtered_in_index_event():
    setup_processor(Config(all_attributes_private = True))

    e = {
        'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
        'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
    }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 3
    check_index_event(output[0], e, filtered_user)
    check_feature_event(output[1], e, False, None)
    check_summary_event(output[2])

def test_feature_event_can_contain_inline_user():
    setup_processor(Config(inline_users_in_events = True))

    e = {
        'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
        'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
    }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 2
    check_feature_event(output[0], e, False, user)
    check_summary_event(output[1])

def test_user_is_filtered_in_feature_event():
    setup_processor(Config(inline_users_in_events = True, all_attributes_private = True))

    e = {
        'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
        'variation': 1, 'value': 'value', 'default': 'default', 'trackEvents': True
    }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 2
    check_feature_event(output[0], e, False, filtered_user)
    check_summary_event(output[1])

def test_event_kind_is_debug_if_flag_is_temporarily_in_debug_mode():
    setup_processor(Config())

    future_time = now() + 100000
    e = {
        'kind': 'feature', 'key': 'flagkey', 'version': 11, 'user': user,
        'variation': 1, 'value': 'value', 'default': 'default',
        'trackEvents': False, 'debugEventsUntilDate': future_time
    }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 3
    check_index_event(output[0], e, user)
    check_feature_event(output[1], e, True, None)
    check_summary_event(output[2])

def test_debug_mode_expires_based_on_client_time_if_client_time_is_later_than_server_time():
    setup_processor(Config())

    # Pick a server time that is somewhat behind the client time
    server_time = now() - 20000

    # Send and flush an event we don't care about, just to set the last server time
    mock_session.set_server_time(server_time)
    ep.send_event({ 'kind': 'identify', 'user': { 'key': 'otherUser' }})
    flush_and_get_events()

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
    output = flush_and_get_events()
    assert len(output) == 2
    check_index_event(output[0], e, user)
    check_summary_event(output[1])

def test_debug_mode_expires_based_on_server_time_if_server_time_is_later_than_client_time():
    setup_processor(Config())

    # Pick a server time that is somewhat ahead of the client time
    server_time = now() + 20000

    # Send and flush an event we don't care about, just to set the last server time
    mock_session.set_server_time(server_time)
    ep.send_event({ 'kind': 'identify', 'user': { 'key': 'otherUser' }})
    flush_and_get_events()

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
    output = flush_and_get_events()
    assert len(output) == 2
    check_index_event(output[0], e, user)
    check_summary_event(output[1])

def test_two_feature_events_for_same_user_generate_only_one_index_event():
    setup_processor(Config())

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

    output = flush_and_get_events()
    assert len(output) == 2
    check_index_event(output[0], e1, user)
    check_summary_event(output[1])

def test_nontracked_events_are_summarized():
    setup_processor(Config())

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

    output = flush_and_get_events()
    assert len(output) == 2
    check_index_event(output[0], e1, user)
    se = output[1]
    assert se['kind'] == 'summary'
    assert se['startDate'] == e1['creationDate']
    assert se['endDate'] == e2['creationDate']
    assert se['features'] == {
        'flagkey1': {
            'default': 'default1',
            'counters': [ { 'version': 11, 'value': 'value1', 'count': 1 } ]
        },
        'flagkey2': {
            'default': 'default2',
            'counters': [ { 'version': 22, 'value': 'value2', 'count': 1 } ]
        }
    }

def test_custom_event_is_queued_with_user():
    setup_processor(Config())

    e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '} }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 2
    check_index_event(output[0], e, user)
    check_custom_event(output[1], e, None)

def test_custom_event_can_contain_inline_user():
    setup_processor(Config(inline_users_in_events = True))

    e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '} }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 1
    check_custom_event(output[0], e, user)

def test_user_is_filtered_in_custom_event():
    setup_processor(Config(inline_users_in_events = True, all_attributes_private = True))

    e = { 'kind': 'custom', 'key': 'eventkey', 'user': user, 'data': { 'thing': 'stuff '} }
    ep.send_event(e)

    output = flush_and_get_events()
    assert len(output) == 1
    check_custom_event(output[0], e, filtered_user)

def test_nothing_is_sent_if_there_are_no_events():
    setup_processor(Config())
    ep.flush()
    assert mock_session.request_data is None

def test_sdk_key_is_sent():
    setup_processor(Config(sdk_key = 'SDK_KEY'))

    ep.send_event({ 'kind': 'identify', 'user': user })
    ep.flush()

    assert mock_session.request_headers.get('Authorization') is 'SDK_KEY'

def test_no_more_payloads_are_sent_after_401_error():
    setup_processor(Config(sdk_key = 'SDK_KEY'))

    mock_session.set_response_status(401)
    ep.send_event({ 'kind': 'identify', 'user': user })
    ep.flush()
    mock_session.clear()

    ep.send_event({ 'kind': 'identify', 'user': user })
    ep.flush()
    assert mock_session.request_data is None


def flush_and_get_events():
    ep.flush()
    return None if mock_session.request_data is None else json.loads(mock_session.request_data)

def check_index_event(data, source, user):
    assert data['kind'] == 'index'
    assert data['creationDate'] == source['creationDate']
    assert data['user'] == user

def check_feature_event(data, source, debug, inline_user):
    assert data['kind'] == ('debug' if debug else 'feature')
    assert data['creationDate'] == source['creationDate']
    assert data['key'] == source['key']
    assert data.get('version') == source.get('version')
    assert data.get('value') == source.get('value')
    assert data.get('default') == source.get('default')
    if inline_user is None:
        assert data['userKey'] == source['user']['key']
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

def check_summary_event(data):
    assert data['kind'] == 'summary'

def now():
    return int(time.time() * 1000)
