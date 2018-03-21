from __future__ import absolute_import

import errno
from threading import Thread

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

import requests
from requests.packages.urllib3.exceptions import ProtocolError

import six

from ldclient.user_filter import UserFilter
from ldclient.interfaces import EventProcessor
from ldclient.util import _headers
from ldclient.util import log


class NullEventProcessor(Thread, EventProcessor):
    def __init(self, config):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def is_alive(self):
        return False

    def send_event(self, event):
        pass

    def flush(self):
        pass

class DefaultEventProcessor(Thread, EventProcessor):
    def __init__(self, config):
        Thread.__init__(self)
        self._session = requests.Session()
        self.daemon = True
        self._config = config
        self._queue = queue.Queue(config.events_max_pending)
        self._user_filter = UserFilter(config)
        self._running = True

    def run(self):
        log.info("Starting event consumer")
        self._running = True
        while self._running:
            try:
                self.send()
            except Exception:
                log.warning(
                    'Unhandled exception in event consumer')

    def stop(self):
        self._running = False

    def send_event(self, event):
        if self._queue.full():
            log.warning("Event queue is full-- dropped an event")
        else:
            self._queue.put(event)

    def flush(self):
        self._queue.join()

    def send_batch(self, events):
        def do_send(should_retry):
            # noinspection PyBroadException
            try:
                output_events = [ self._make_output_event(e) for e in events ]
                json_body = jsonpickle.encode(output_events, unpicklable=False)
                log.debug('Sending events payload: ' + json_body)
                hdrs = _headers(self._config.sdk_key)
                uri = self._config.events_uri
                r = self._session.post(uri,
                                       headers=hdrs,
                                       timeout=(self._config.connect_timeout, self._config.read_timeout),
                                       data=json_body)
                if r.status_code == 401:
                    log.error('Received 401 error, no further events will be posted since SDK key is invalid')
                    self.stop()
                    return
                r.raise_for_status()
            except ProtocolError as e:
                if e.args is not None and len(e.args) > 1 and e.args[1] is not None:
                    inner = e.args[1]
                    if inner.errno is not None and inner.errno == errno.ECONNRESET and should_retry:
                        log.warning(
                            'ProtocolError exception caught while sending events. Retrying.')
                        do_send(False)
                else:
                    log.warning(
                        'Unhandled exception in event consumer. Analytics events were not processed.',
                        exc_info=True)
            except:
                log.warning(
                    'Unhandled exception in event consumer. Analytics events were not processed.',
                    exc_info=True)

        try:
            do_send(True)
        finally:
            for _ in events:
                self._queue.task_done()

    def _make_output_event(self, e):
        kind = e['kind']
        if kind == 'feature':
            is_debug = (not e['trackEvents']) and (e.get('debugEventsUntilDate') is not None)
            out = {
                'kind': 'debug' if is_debug else 'feature',
                'creationDate': e['creationDate'],
                'key': e['key'],
                'version': e.get('version'),
                'value': e.get('value'),
                'default': e.get('default'),
                'prereqOf': e.get('prereqOf')
            }
            if self._config.inline_users_in_events:
                out['user'] = self._user_filter.filter_user_props(e['user'])
            else:
                out['userKey'] = e['user'].get('key')
            return out
        elif kind == 'identify':
            return {
                'kind': 'identify',
                'creationDate': e['creationDate'],
                'user': self._user_filter.filter_user_props(e['user'])
            }
        elif kind == 'custom':
            out = {
                'kind': 'custom',
                'key': e['key'],
                'data': e.get('data')
            }
            if self._config.inline_users_in_events:
                out['user'] = self._user_filter.filter_user_props(e['user'])
            else:
                out['userKey'] = e['user'].get('key')
            return out
        else:
            return e

    def send(self):
        events = self.next()

        if len(events) == 0:
            return
        else:
            self.send_batch(events)

    def next(self):
        q = self._queue
        items = []

        item = self.next_item()
        if item is None:
            return items

        items.append(item)
        while len(items) < self._config.events_upload_max_batch_size and not q.empty():
            item = self.next_item()
            if item:
                items.append(item)

        return items

    def next_item(self):
        q = self._queue
        # noinspection PyBroadException
        try:
            item = q.get(block=True, timeout=5)
            return item
        except Exception:
            return None
