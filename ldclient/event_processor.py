from __future__ import absolute_import

from collections import namedtuple
from email.utils import parsedate
import errno
import jsonpickle
from threading import Event, Lock, Thread
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
from ldclient.user_deduplicator import UserDeduplicator
from ldclient.user_filter import UserFilter
from ldclient.interfaces import EventProcessor
from ldclient.repeating_timer import RepeatingTimer
from ldclient.util import _headers
from ldclient.util import log


class NullEventProcessor(EventProcessor):
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


EventProcessorMessage = namedtuple('EventProcessorMessage', ['type', 'param'])


class EventOutputFormatter(object):
    def __init__(self, config):
        self._config = config
        self._user_filter = UserFilter(config)

    """
    Transform an event into the format used for the event payload.
    """
    def make_output_event(self, e):
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
                'creationDate': e['creationDate'],
                'key': e['key'],
                'data': e.get('data')
            }
            if self._config.inline_users_in_events:
                out['user'] = self._user_filter.filter_user_props(e['user'])
            else:
                out['userKey'] = e['user'].get('key')
            return out
        elif kind == 'index':
            return {
                'kind': 'index',
                'creationDate': e['creationDate'],
                'user': self._user_filter.filter_user_props(e['user'])
            }
        else:
            return e

    """
    Transform summarizer data into the format used for the event payload.
    """
    def make_summary_event(self, summary):
        flags_out = dict()
        for ckey, cval in summary.counters.items():
            flag_key, variation, version = ckey
            flag_data = flags_out.get(flag_key)
            if flag_data is None:
                flag_data = { 'default': cval['default'], 'counters': [] }
                flags_out[flag_key] = flag_data
            counter = {
                'count': cval['count'],
                'value': cval['value']
            }
            if version is None:
                counter['unknown'] = True
            else:
                counter['version'] = version
            flag_data['counters'].append(counter)
        return {
            'kind': 'summary',
            'startDate': summary.start_date,
            'endDate': summary.end_date,
            'features': flags_out
        }


class EventPayloadSendTask(object):
    def __init__(self, session, config, events, summary, response_fn, completion_fn):
        self._session = session
        self._config = config
        self._events = events
        self._summary = summary
        self._response_fn = response_fn
        self._completion_fn = completion_fn
    
    def start(self):
        Thread(target = self._run).start()

    def _run(self):
        try:
            formatter = EventOutputFormatter(self._config)
            output_events = [ formatter.make_output_event(e) for e in self._events ]
            if len(self._summary.counters) > 0:
                output_events.append(formatter.make_summary_event(self._summary))
            resp = self._do_send(output_events, True)
            if resp is not None:
                self._response_fn(resp)
        except:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)
        finally:
            self._completion_fn()

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
            r.raise_for_status()
            return r
        except ProtocolError as e:
            if e.args is not None and len(e.args) > 1 and e.args[1] is not None:
                inner = e.args[1]
                if inner.errno is not None and inner.errno == errno.ECONNRESET and should_retry:
                    log.warning(
                        'ProtocolError exception caught while sending events. Retrying.')
                    self._do_send(output_events, False)
            else:
                log.warning(
                    'Unhandled exception in event processor. Analytics events were not processed.',
                    exc_info=True)
        except:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)


class EventDispatcher(object):
    def __init__(self, queue, config, session):
        self._queue = queue
        self._config = config
        self._session = requests.Session() if session is None else session
        self._disabled = False
        self._events = []
        self._summarizer = EventSummarizer()
        self._user_deduplicator = UserDeduplicator(config)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0

        self._active_flush_workers_lock = Lock()
        self._active_flush_workers_count = 0
        self._active_flush_workers_event = Event()

        self._main_thread = Thread(target=self._run_main_loop)
        self._main_thread.daemon = True
        self._main_thread.start()

    def _run_main_loop(self):
        log.info("Starting event processor")
        while True:
            try:
                message = self._queue.get(block=True)
                if message.type == 'event':
                    self._process_event(message.param)
                elif message.type == 'flush':
                    self._trigger_flush()
                elif message.type == 'flush_users':
                    self._user_deduplicator.reset_users()
                elif message.type == 'test_sync':
                    self._wait_until_inactive()
                    message.param.set()
                elif message.type == 'stop':
                    self._do_shutdown()
                    message.param.set()
                    return
            except Exception:
                log.error('Unhandled exception in event processor', exc_info=True)
        self._session.close()
    
    def _process_event(self, event):
        if self._disabled:
            return

        # For each user we haven't seen before, we add an index event - unless this is already
        # an identify event for that user.
        user = event.get('user')
        if not self._config.inline_users_in_events and user and not self._user_deduplicator.notice_user(user):
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
                now = int(time.time() * 1000)
                if debug_until > last_past and debug_until > now:
                    return True
            return False
        else:
            return True

    def _trigger_flush(self):
        if self._disabled:
            return
        events = self._events
        self._events = []
        snapshot = self._summarizer.snapshot()
        if len(events) > 0 or len(snapshot.counters) > 0:
            with self._active_flush_workers_lock:
                # TODO: if we're at the limit, don't start a task and don't clear the state
                self._active_flush_workers_count = self._active_flush_workers_count + 1
            task = EventPayloadSendTask(self._session, self._config, events, snapshot,
                self._handle_response, self._release_flush_worker)
            task.start()

    def _handle_response(self, r):
        server_date_str = r.headers.get('Date')
        if server_date_str is not None:
            server_date = parsedate(server_date_str)
            if server_date is not None:
                self._last_known_past_time = server_date
        if r.status_code == 401:
            log.error('Received 401 error, no further events will be posted since SDK key is invalid')
            self._disabled = True
            return

    def _release_flush_worker(self):
        with self._active_flush_workers_lock:
            self._active_flush_workers_count = self._active_flush_workers_count - 1
            self._active_flush_workers_event.clear()
            self._active_flush_workers_event.set()
    
    def _wait_until_inactive(self):
        while True:
            with self._active_flush_workers_lock:
                if self._active_flush_workers_count == 0:
                    return
            self._active_flush_workers_event.wait()

    def _do_shutdown(self):
        self._wait_until_inactive()
        self._session.close()


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config, session=None):
        self._queue = queue.Queue(config.events_max_pending)
        self._dispatcher = EventDispatcher(self._queue, config, session)
        self._flush_timer = RepeatingTimer(config.flush_interval, self.flush)
        self._users_flush_timer = RepeatingTimer(config.user_keys_flush_interval, self._flush_users)
        self._flush_timer.start()
        self._users_flush_timer.start()

    def send_event(self, event):
        event['creationDate'] = int(time.time() * 1000)
        self._queue.put(EventProcessorMessage('event', event))

    def flush(self):
        self._queue.put(EventProcessorMessage('flush', None))

    def stop(self):
        self._flush_timer.stop()
        self._users_flush_timer.stop()
        self.flush()
        self._post_message_and_wait('stop')

    def _flush_users(self):
        self._queue.put(EventProcessorMessage('flush_users', None))

    # Used only in tests
    def _wait_until_inactive(self):
        self._post_message_and_wait('test_sync')

    def _post_message_and_wait(self, type):
        reply = Event()
        self._queue.put(EventProcessorMessage(type, reply))
        reply.wait()
