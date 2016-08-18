from __future__ import absolute_import

import errno
import json

import txrequests
from cachecontrol import CacheControl
from queue import Empty
from requests.packages.urllib3.exceptions import ProtocolError
from twisted.internet import task, defer

from ldclient.interfaces import EventConsumer
from ldclient.util import _headers, log


class TwistedEventConsumer(EventConsumer):

    def __init__(self, queue, sdk_key, config):
        self._queue = queue
        """ :type: queue.Queue """

        self._session = CacheControl(txrequests.Session())
        """ :type: txrequests.Session """

        self._sdk_key = sdk_key
        self._config = config
        """ :type: ldclient.twisted.TwistedConfig """

        self._looping_call = None
        """ :type: LoopingCall"""

    def start(self):
        self._looping_call = task.LoopingCall(self._consume)
        self._looping_call.start(5)

    def stop(self):
        self._looping_call.stop()

    def is_alive(self):
        return self._looping_call is not None and self._looping_call.running

    def flush(self):
        return self._consume()

    def _consume(self):
        items = []
        try:
            while True:
                items.append(self._queue.get_nowait())
        except Empty:
            pass

        if items:
            return self.send_batch(items)

    @defer.inlineCallbacks
    def send_batch(self, events):
        @defer.inlineCallbacks
        def do_send(should_retry):
            # noinspection PyBroadException
            try:
                if isinstance(events, dict):
                    body = [events]
                else:
                    body = events
                hdrs = _headers(self._sdk_key)
                r = yield self._session.post(self._config.events_uri,
                                             headers=hdrs,
                                             timeout=(self._config.connect_timeout, self._config.read_timeout),
                                             data=json.dumps(body))
                r.raise_for_status()
            except ProtocolError as e:
                inner = e.args[1]
                if inner.errno == errno.ECONNRESET and should_retry:
                    log.warning(
                        'ProtocolError exception caught while sending events. Retrying.')
                    yield do_send(False)
                else:
                    log.exception(
                        'Unhandled exception in event consumer. Analytics events were not processed.')
            except:
                log.exception(
                    'Unhandled exception in event consumer. Analytics events were not processed.')
        try:
            yield do_send(True)
        finally:
            for _ in events:
                self._queue.task_done()
