from __future__ import absolute_import
from functools import partial

import json
from queue import Empty
import errno
from cachecontrol import CacheControl
from ldclient import LDClient, _headers, log, _evaluate, _stream_headers, StreamProcessor, Config
from ldclient.twisted_sse import TwistedSSEClient
from requests.packages.urllib3.exceptions import ProtocolError
from twisted.internet import task, defer
import txrequests


class TwistedLDClient(LDClient):
    def __init__(self, api_key, config=None):
        if config is None:
            config = TwistedConfig.default()
        super(TwistedLDClient, self).__init__(api_key, config)
        self._session = CacheControl(txrequests.Session())

    def _check_consumer(self):
        if not self._consumer or not self._consumer.is_alive():
            self._consumer = TwistedConsumer(self._session, self._queue, self._api_key, self._config)
            self._consumer.start()

    def flush(self):
        if self._offline:
            return defer.succeed(True)
        self._check_consumer()
        return self._consumer.flush()

    def toggle(self, key, user, default=False):
        @defer.inlineCallbacks
        def run(should_retry):
            # noinspection PyBroadException
            try:
                if self._offline:
                    defer.returnValue(default)
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
        if self._config.stream and self._stream_processor.initialized():
            feature = self._stream_processor.get_feature(key)
        else:
            hdrs = _headers(self._api_key)
            uri = self._config.base_uri + '/api/eval/features/' + key
            r = yield self._session.get(uri, headers=hdrs, timeout=(self._config.connect, self._config.read))
            r.raise_for_status()
            feature = r.json()
        val = _evaluate(feature, user)
        if val is None:
            val = default
        defer.returnValue(val)


class TwistedConfig(Config):
    def __init__(self, *args, **kwargs):
        super(TwistedConfig, self).__init__(*args, **kwargs)
        self.stream_processor_class = TwistedStreamProcessor


class TwistedStreamProcessor(object):

    def __init__(self, api_key, config):
        self._store = config.feature_store_class()
        self.sse_client = TwistedSSEClient(config.stream_uri + "/", headers=_stream_headers(api_key,
                                                                                            "PythonTwistedClient"),
                                           verify=config.verify,
                                           on_event=partial(StreamProcessor.process_message, self._store))
        self.running = False

    def start(self):
        self.sse_client.start()
        self.running = True

    def stop(self):
        self.sse_client.stop()

    def get_feature(self, key):
        return self._store.get(key)

    def initialized(self):
        return self._store.initialized()

    def is_alive(self):
        return self.running


class TwistedConsumer(object):
    def __init__(self, session, queue, api_key, config):
        self._queue = queue
        """ @type: queue.Queue """

        self._session = session
        """ :type: txrequests.Session """

        self._api_key = api_key
        self._config = config
        """ :type: ldclient.twisted.TwistedConfig """

        self._looping_call = None
        """ :type: LoopingCall"""
        self._flushed = None

    def start(self):
        self._flushed = defer.Deferred()
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
                hdrs = _headers(self._api_key)
                uri = self._config.base_uri + '/api/events/bulk'
                r = yield self._session.post(uri, headers=hdrs, timeout=(self._config.connect, self._config.read),
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