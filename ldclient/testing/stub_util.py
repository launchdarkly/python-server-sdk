import json
from email.utils import formatdate

from ldclient.impl.model import ModelEntity
from ldclient.interfaces import (EventProcessor, FeatureRequester,
                                 FeatureStore, UpdateProcessor)
from ldclient.testing.http_util import ChunkedResponse, JsonResponse


def item_as_json(item):
    return item.to_json_dict() if isinstance(item, ModelEntity) else item


def make_items_map(items=[]):
    ret = {}
    for item in items:
        ret[item['key']] = item_as_json(item)
    return ret


def make_put_event(flags=[], segments=[]):
    data = {"data": {"flags": make_items_map(flags), "segments": make_items_map(segments)}}
    return 'event:put\ndata: %s\n\n' % json.dumps(data)


def make_invalid_put_event():
    return 'event:put\ndata: {"data": {\n\n'


def make_patch_event(kind, item):
    path = '%s%s' % (kind.stream_api_path, item['key'])
    data = {"path": path, "data": item_as_json(item)}
    return 'event:patch\ndata: %s\n\n' % json.dumps(data)


def make_delete_event(kind, key, version):
    path = '%s%s' % (kind.stream_api_path, key)
    data = {"path": path, "version": version}
    return 'event:delete\ndata: %s\n\n' % json.dumps(data)


def stream_content(event=None):
    stream = ChunkedResponse({'Content-Type': 'text/event-stream'})
    if event:
        stream.push(event)
    return stream


def poll_content(flags=[], segments=[]):
    data = {"flags": make_items_map(flags), "segments": make_items_map(segments)}
    return JsonResponse(data)


class MockEventProcessor(EventProcessor):
    def __init__(self, *_):
        self._running = False
        self._events = []

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
        self.request_count = 0

    def get_all_data(self):
        self.request_count += 1
        if self.exception is not None:
            raise self.exception
        return self.all_data


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


class CapturingFeatureStore(FeatureStore):
    def init(self, all_data):
        self.data = all_data

    def get(self, kind, key, callback=lambda x: x):
        pass

    def all(self, kind, callback=lambda x: x):
        pass

    def delete(self, kind, key, version):
        pass

    def upsert(self, kind, item):
        pass

    @property
    def initialized(self):
        return True

    @property
    def received_data(self):
        return self.data
