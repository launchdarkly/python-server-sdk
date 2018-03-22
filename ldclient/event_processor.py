from __future__ import absolute_import

from email.utils import parsedate
import errno
import jsonpickle
from threading import Event, Thread, Timer
import time

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

import requests
from requests.packages.urllib3.exceptions import ProtocolError

import six

from ldclient.event_summarizer import EventSummarizer
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
    def __init__(self, config, session=None):
        Thread.__init__(self)
        self._session = requests.Session() if session is None else session
        self.daemon = True
        self._config = config
        self._queue = queue.Queue(config.events_max_pending)
        self._events = []
        self._user_filter = UserFilter(config)
        self._summarizer = EventSummarizer(config)
        self._last_known_past_time = 0
        self._running = True
        self._set_flush_timer()
        self._set_users_flush_timer()

    def run(self):
        log.info("Starting event consumer")
        self._running = True
        while self._running:
            try:
                self._process_next()
                log.error("*** processed ***")
            except Exception:
                log.error('Unhandled exception in event consumer', exc_info=True)

    def stop(self):
        log.error("*** GOT STOP ***")
        self.flush()
        self._session.close()
        self._running = False
        self._flush_timer.cancel()
        self._users_flush_timer.cancel()
        # Post a non-message so we won't keep blocking on the queue
        self._queue.put(('stop', None))

    def send_event(self, event):
        event['creationDate'] = self._now()
        self._queue.put(('event', event))

    def flush(self):
        # Put a flush message on the queue and wait until it's been processed.
        reply = Event()
        self._queue.put(('flush', reply))
        reply.wait()

    def _now(self):
        return int(time.time() * 1000)

    def _set_flush_timer(self):
        self._flush_timer = Timer(5, self._flush_async)
        self._flush_timer.start()

    def _set_users_flush_timer(self):
        self._users_flush_timer = Timer(self._config.user_keys_flush_interval, self._flush_users)
        self._users_flush_timer.start()

    def _flush_async(self):
        self._queue.put(('flush', None))
        self._set_flush_timer()

    def _flush_users(self):
        self._queue.put(('flush_users', None))
        self._set_users_flush_timer()

    def _process_next(self):
        item = self._queue.get(block=True)
        if item[0] == 'event':
            self._process_event(item[1])
        elif item[0] == 'flush':
            self._dispatch_flush(item[1])
        elif item[0] == 'flush_users':
            self._summarizer.reset_users()

    def _process_event(self, event):
        # For each user we haven't seen before, we add an index event - unless this is already
        # an identify event for that user.
        user = event.get('user')
        if not self._config.inline_users_in_events and user and not self._summarizer.notice_user(user):
            if event['kind'] != 'identify':
                ie = { 'kind': 'index', 'creationDate': event['creationDate'], 'user': user }
                self._store_event(ie)

        # Always record the event in the summarizer.
        self._summarizer.summarize_event(event)

        if self._should_track_full_event(event):
            # Queue the event as-is; we'll transform it into an output event when we're flushing
            # (to avoid doing that work on our main thread).
            self._store_event(event)

    def _store_event(self, event):
        if len(self._events) >= self._config.events_max_pending:
            log.warning("Event queue is full-- dropped an event")
        else:
            self._events.append(event)

    def _should_track_full_event(self, event):
        if event['kind'] == 'feature':
            if event.get('trackEvents'):
                return True
            debug_until = event.get('debugEventsUntilDate')
            if debug_until is not None:
                last_past = self._last_known_past_time
                if debug_until > last_past and debug_until > self._now():
                    return True
            return False
        else:
            return True

    def _dispatch_flush(self, reply):
        events = self._events
        self._events = []
        snapshot = self._summarizer.snapshot()
        if len(events) > 0 or len(snapshot['counters']) > 0:
            flusher = Thread(target = self._flush_task, args=(events, snapshot, reply))
            flusher.start()
        else:
            if reply is not None:
                reply.set()

    def _flush_task(self, events, snapshot, reply):
        output_events = [ self._make_output_event(e) for e in events ]
        if len(snapshot['counters']) > 0:
            summary = self._summarizer.output(snapshot)
            summary['kind'] = 'summary'
            output_events.append(summary)
        try:
            self._do_send(output_events, True)
        finally:
            if reply is not None:
                reply.set()

    def _do_send(self, output_events, should_retry):
        # noinspection PyBroadException
        try:
            json_body = jsonpickle.encode(output_events, unpicklable=False)
            log.debug('Sending events payload: ' + json_body)
            hdrs = _headers(self._config.sdk_key)
            uri = self._config.events_uri
            r = self._session.post(uri,
                                   headers=hdrs,
                                   timeout=(self._config.connect_timeout, self._config.read_timeout),
                                   data=json_body)
            server_date_str = r.headers.get('Date')
            if server_date_str is not None:
                server_date = parsedate(server_date_str)
                if server_date is not None:
                    self._last_known_past_time = server_date
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
                    self.do_send(output_events, False)
            else:
                log.warning(
                    'Unhandled exception in event consumer. Analytics events were not processed.',
                    exc_info=True)
        except:
            log.warning(
                'Unhandled exception in event consumer. Analytics events were not processed.',
                exc_info=True)

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
