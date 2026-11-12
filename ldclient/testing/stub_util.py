import gzip
from email.utils import formatdate

from ldclient.interfaces import EventProcessor, FeatureStore
from ldclient.testing.http_util import ChunkedResponse


def decode_request_body(headers, body):
    """Return the request body. Decompress the body if the headers show gzip encoding."""
    if headers is not None and headers.get('Content-Encoding') == 'gzip':
        return gzip.decompress(body)
    return body


def stream_content(event=None, headers=None):
    stream_headers = {'Content-Type': 'text/event-stream'}
    stream_headers.update(headers or {})
    stream = ChunkedResponse(stream_headers)
    if event:
        stream.push(event)
    return stream


class MockEventProcessor(EventProcessor):
    def __init__(self, *_):
        self._running = False
        self._events = []

    def stop(self):
        self._running = False

    def start(self):
        self._running = True

    def send_event(self, event):
        self._events.append(event)

    def flush(self):
        pass


class _MockHTTPHeaderDict(dict):
    def __init__(self, d):
        super().__init__({k.lower(): v for k, v in d.items()})

    def get(self, key, default=None):
        return super().get(key.lower(), default)


class MockResponse:
    def __init__(self, status, headers):
        self._status = status
        self._headers = _MockHTTPHeaderDict(headers)

    @property
    def status(self):
        return self._status

    @property
    def headers(self):
        return self._headers

    @property
    def data(self):
        # The HTTPTransport shim reads the body of every response
        return b''


class MockHttp:
    def __init__(self):
        self._recorded_requests = []
        self._request_data = None
        self._request_headers = None
        self._response_func = None
        self._response_status = 200
        self._server_time = None

    def request(self, method, uri, headers, timeout, body, retries):
        self._recorded_requests.append((headers, body))
        resp_hdr = dict()
        if self._server_time is not None:
            resp_hdr['date'] = formatdate(self._server_time / 1000, localtime=False, usegmt=True)
        if self._response_func is not None:
            return self._response_func()
        return MockResponse(self._response_status, resp_hdr)

    def clear(self):
        pass

    @property
    def request_data(self):
        """Return the body of the last request. Decompress the body if it is gzip-encoded."""
        if len(self._recorded_requests) != 0:
            return decode_request_body(*self._recorded_requests[-1])

    @property
    def raw_request_data(self):
        """Return the body of the last request as the SDK sent it."""
        if len(self._recorded_requests) != 0:
            return self._recorded_requests[-1][1]

    @property
    def request_headers(self):
        if len(self._recorded_requests) != 0:
            return self._recorded_requests[-1][0]

    @property
    def recorded_requests(self):
        return self._recorded_requests

    def set_response_status(self, status):
        self._response_status = status

    def set_response_func(self, response_func):
        self._response_func = response_func

    def set_server_time(self, timestamp):
        self._server_time = timestamp

    def reset(self):
        self._recorded_requests = []


class CapturingFeatureStore(FeatureStore):
    def init(self, all_data):
        self.data = all_data

    def get(self, kind, key, callback=lambda x: x):
        pass

    def all(self, kind, callback=lambda x: x):
        pass

    def delete(self, kind, key, version):
        return False

    def upsert(self, kind, item):
        return False

    @property
    def initialized(self):
        return True

    def is_monitoring_enabled(self):
        return False

    def is_available(self):
        return True

    def close(self):
        pass

    @property
    def received_data(self):
        return self.data
