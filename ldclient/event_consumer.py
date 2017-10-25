from __future__ import absolute_import

import errno
from threading import Thread

import requests
from requests.packages.urllib3.exceptions import ProtocolError

from ldclient.event_serializer import EventSerializer
from ldclient.interfaces import EventConsumer
from ldclient.util import _headers
from ldclient.util import log


class EventConsumerImpl(Thread, EventConsumer):
    def __init__(self, event_queue, config):
        Thread.__init__(self)
        self._session = requests.Session()
        self.daemon = True
        self._config = config
        self._queue = event_queue
        self._serializer = EventSerializer(config)
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

    def flush(self):
        self._queue.join()

    def send_batch(self, events):
        def do_send(should_retry):
            # noinspection PyBroadException
            try:
                json_body = self._serializer.serialize_events(events)
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
