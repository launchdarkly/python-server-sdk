import json
from queue import Empty
import errno
from cachecontrol import CacheControl
from ldclient import LDClient, _headers, log, _evaluate
from requests.packages.urllib3.exceptions import ProtocolError
from twisted.internet import task, defer
import txrequests


class TwistedLDClient(LDClient):
    def __init__(self, api_key, config=None):
        super().__init__(api_key, config)
        self._session = CacheControl(txrequests.Session())

    def _check_consumer(self):
        if not self._consumer or not self._consumer.is_alive():
            self._consumer = TwistedConsumer(self._session, self._queue, self._api_key, self._config)
            self._consumer.start()

    def flush(self):
        if self._offline:
            print("offline")
            return defer.succeed(True)
        self._check_consumer()
        return self._consumer.flush()

    def toggle(self, key, user, default=False):
        @defer.inlineCallbacks
        def run(should_retry):
            # noinspection PyBroadException
            try:
                if self._offline:
                    return default
                val = yield self._toggle(key, user, default)
                self._send({'kind': 'feature', 'key': key, 'user': user, 'value': val})
                defer.returnValue(val)
            except ProtocolError as e:
                inner = e.args[1]
                if inner.errno == errno.ECONNRESET and should_retry:
                    log.warning('ProtocolError exception caught while getting flag. Retrying.')
                    d = yield run(False)
                    defer.returnValue(d)
                else:
                    log.exception('Unhandled exception. Returning default value for flag.')
                    defer.returnValue(default)
            except Exception:
                log.exception('Unhandled exception. Returning default value for flag.')
                defer.returnValue(default)

        return run(True)

    @defer.inlineCallbacks
    def _toggle(self, key, user, default):
        hdrs = _headers(self._api_key)
        uri = self._config._base_uri + '/api/eval/features/' + key
        r = yield self._session.get(uri, headers=hdrs, timeout=(self._config._connect, self._config._read))
        r.raise_for_status()
        hash = r.json()
        val = _evaluate(hash, user)
        if val is None:
            val = default
        defer.returnValue(val)


class TwistedConsumer(object):
    def __init__(self, session, queue, api_key, config):
        self._queue = queue
        """ @type: queue.Queue """
        self._session = session
        """ :type: txrequests.Session """

        self._api_key = api_key
        self._config = config
        self._flushed = None
        """ :type: Deferred """
        self._looping_call = None
        """ :type: LoopingCall"""

    def start(self):
        self._flushed = defer.Deferred()
        self._looping_call = task.LoopingCall(self._consume)
        self._looping_call.start(5)

    def stop(self):
        self._looping_call.stop()

    def is_alive(self):
        return self._looping_call is not None and self._looping_call.running

    def flush(self):
        return self._flushed

    def _consume(self):
        items = []
        try:
            while True:
                items.append(self._queue.get_nowait())
        except Empty:
            pass

        if items:
            def on_batch_done(*_):
                print("========== batch done")
                self._flushed.callback(True)
                self._flushed = defer.Deferred()

            d = self.send_batch(items)
            """ :type: Deferred """
            d.addBoth(on_batch_done)

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
                hdrs = _headers(self._api_key)
                uri = self._config._base_uri + '/api/events/bulk'
                r = yield self._session.post(uri, headers=hdrs, timeout=(self._config._connect, self._config._read),
                                             data=json.dumps(body))
                r.raise_for_status()
            except ProtocolError as e:
                inner = e.args[1]
                if inner.errno == errno.ECONNRESET and should_retry:
                    log.warning('ProtocolError exception caught while sending events. Retrying.')
                    yield do_send(False)
                else:
                    log.exception('Unhandled exception in event consumer. Analytics events were not processed.')
            except:
                log.exception('Unhandled exception in event consumer. Analytics events were not processed.')
        try:
            yield do_send(True)
        finally:
            for _ in events:
                self._queue.task_done()