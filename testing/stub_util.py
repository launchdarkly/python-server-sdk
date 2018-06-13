from email.utils import formatdate
from requests.structures import CaseInsensitiveDict

from ldclient.interfaces import EventProcessor, FeatureRequester, UpdateProcessor


class MockEventProcessor(EventProcessor):
    def __init__(self, *_):
        self._running = False
        self._events = []
        mock_event_processor = self

    def stop(self):
        self._running = False

    def start(self):
        self._running = True

    def is_alive(self):
        return self._running

    def send_event(self, event):
        self._events.append(event)

    def flush(self):
        pass

class MockFeatureRequester(FeatureRequester):
    def __init__(self):
        self.all_data = {}
        self.exception = None

    def get_all_data(self):
        if self.exception is not None:
            raise self.exception
        return self.all_data

    def get_one(self, kind, key):
        pass

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

class MockUpdateProcessor(UpdateProcessor):
    def __init__(self, config, store, ready):
        ready.set()

    def start(self):
        pass

    def stop(self):
        pass

    def is_alive(self):
        return True

    def initialized(self):
        return True
