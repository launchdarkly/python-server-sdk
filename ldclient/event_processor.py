from __future__ import absolute_import

from collections import namedtuple
from email.utils import parsedate
import errno
import jsonpickle
from threading import Event, Lock, Thread
import six
import time
import urllib3

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

from ldclient.event_summarizer import EventSummarizer
from ldclient.fixed_thread_pool import FixedThreadPool
from ldclient.lru_cache import SimpleLRUCache
from ldclient.user_filter import UserFilter
from ldclient.interfaces import EventProcessor
from ldclient.repeating_timer import RepeatingTimer
from ldclient.util import UnsuccessfulResponseException
from ldclient.util import _headers
from ldclient.util import create_http_pool_manager
from ldclient.util import log
from ldclient.util import http_error_message, is_http_error_recoverable, throw_if_unsuccessful_response


__MAX_FLUSH_THREADS__ = 5
__CURRENT_EVENT_SCHEMA__ = 3

class NullEventProcessor(EventProcessor):
    def __init__(self):
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
        self._inline_users = config.inline_users_in_events
        self._user_filter = UserFilter(config)

    def make_output_events(self, events, summary):
        events_out = [ self.make_output_event(e) for e in events ]
        if len(summary.counters) > 0:
            events_out.append(self.make_summary_event(summary))
        return events_out
    
    def make_output_event(self, e):
        kind = e['kind']
        if kind == 'feature':
            is_debug = e.get('debug')
            out = {
                'kind': 'debug' if is_debug else 'feature',
                'creationDate': e['creationDate'],
                'key': e['key'],
                'version': e.get('version'),
                'variation': e.get('variation'),
                'value': e.get('value'),
                'default': e.get('default'),
                'prereqOf': e.get('prereqOf')
            }
            if self._inline_users or is_debug:
                out['user'] = self._user_filter.filter_user_props(e['user'])
            else:
                out['userKey'] = e['user'].get('key')
            if e.get('reason'):
                out['reason'] = e.get('reason')
            return out
        elif kind == 'identify':
            return {
                'kind': 'identify',
                'creationDate': e['creationDate'],
                'key': e['user'].get('key'),
                'user': self._user_filter.filter_user_props(e['user'])
            }
        elif kind == 'custom':
            out = {
                'kind': 'custom',
                'creationDate': e['creationDate'],
                'key': e['key'],
                'data': e.get('data')
            }
            if self._inline_users:
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
            if variation is not None:
                counter['variation'] = variation
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
    def __init__(self, http, config, formatter, payload, response_fn):
        self._http = http
        self._config = config
        self._formatter = formatter
        self._payload = payload
        self._response_fn = response_fn

    def run(self):
        try:
            output_events = self._formatter.make_output_events(self._payload.events, self._payload.summary)
            resp = self._do_send(output_events)
        except Exception:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)

    def _do_send(self, output_events):
        # noinspection PyBroadException
        try:
            json_body = jsonpickle.encode(output_events, unpicklable=False)
            log.debug('Sending events payload: ' + json_body)
            hdrs = _headers(self._config.sdk_key)
            hdrs['X-LaunchDarkly-Event-Schema'] = str(__CURRENT_EVENT_SCHEMA__)
            uri = self._config.events_uri
            r = self._http.request('POST', uri,
                                   headers=hdrs,
                                   timeout=urllib3.Timeout(connect=self._config.connect_timeout, read=self._config.read_timeout),
                                   body=json_body,
                                   retries=1)
            self._response_fn(r)
            return r
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed. [%s]', e)


FlushPayload = namedtuple('FlushPayload', ['events', 'summary'])


class EventBuffer(object):
    def __init__(self, capacity):
        self._capacity = capacity
        self._events = []
        self._summarizer = EventSummarizer()
        self._exceeded_capacity = False
    
    def add_event(self, event):
        if len(self._events) >= self._capacity:
            if not self._exceeded_capacity:
                log.warning("Event queue is full-- dropped an event")
                self._exceeded_capacity = True
        else:
            self._events.append(event)
            self._exceeded_capacity = False
    
    def add_to_summary(self, event):
        self._summarizer.summarize_event(event)
    
    def get_payload(self):
        return FlushPayload(self._events, self._summarizer.snapshot())
    
    def clear(self):
        self._events = []
        self._summarizer.clear()


class EventDispatcher(object):
    def __init__(self, queue, config, http_client):
        self._queue = queue
        self._config = config
        self._http = create_http_pool_manager(num_pools=1, verify_ssl=config.verify_ssl) if http_client is None else http_client
        self._close_http = (http_client is None)  # so we know whether to close it later
        self._disabled = False
        self._buffer = EventBuffer(config.events_max_pending)
        self._user_keys = SimpleLRUCache(config.user_keys_capacity)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0

        self._flush_workers = FixedThreadPool(__MAX_FLUSH_THREADS__, "ldclient.flush")

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
                    self._user_keys.clear()
                elif message.type == 'test_sync':
                    self._flush_workers.wait()
                    message.param.set()
                elif message.type == 'stop':
                    self._do_shutdown()
                    message.param.set()
                    return
            except Exception:
                log.error('Unhandled exception in event processor', exc_info=True)
    
    def _process_event(self, event):
        if self._disabled:
            return

        # Always record the event in the summarizer.
        self._buffer.add_to_summary(event)

        # Decide whether to add the event to the payload. Feature events may be added twice, once for
        # the event (if tracked) and once for debugging.
        add_full_event = False
        add_debug_event = False
        add_index_event = False
        if event['kind'] == "feature":
            add_full_event = event['trackEvents']
            add_debug_event = self._should_debug_event(event)
        else:
            add_full_event = True

        # For each user we haven't seen before, we add an index event - unless this is already
        # an identify event for that user.
        if not (add_full_event and self._config.inline_users_in_events):
            user = event.get('user')
            if user and not self.notice_user(user):
                if event['kind'] != 'identify':
                    add_index_event = True

        if add_index_event:
            ie = { 'kind': 'index', 'creationDate': event['creationDate'], 'user': user }
            self._buffer.add_event(ie)
        if add_full_event:
            self._buffer.add_event(event)
        if add_debug_event:
            debug_event = event.copy()
            debug_event['debug'] = True
            self._buffer.add_event(debug_event)

    # Add to the set of users we've noticed, and return true if the user was already known to us.
    def notice_user(self, user):
        if user is None or 'key' not in user:
            return False
        key = user['key']
        return self._user_keys.put(key, True)

    def _should_debug_event(self, event):
        debug_until = event.get('debugEventsUntilDate')
        if debug_until is not None:
            last_past = self._last_known_past_time
            now = int(time.time() * 1000)
            if debug_until > last_past and debug_until > now:
                return True
        return False

    def _trigger_flush(self):
        if self._disabled:
            return
        payload = self._buffer.get_payload()
        if len(payload.events) > 0 or len(payload.summary.counters) > 0:
            task = EventPayloadSendTask(self._http, self._config, self._formatter, payload,
                self._handle_response)
            if self._flush_workers.execute(task.run):
                # The events have been handed off to a flush worker; clear them from our buffer.
                self._buffer.clear()
            else:
                # We're already at our limit of concurrent flushes; leave the events in the buffer.
                pass

    def _handle_response(self, r):
        server_date_str = r.getheader('Date')
        if server_date_str is not None:
            server_date = parsedate(server_date_str)
            if server_date is not None:
                timestamp = int(time.mktime(server_date) * 1000)
                self._last_known_past_time = timestamp
        if r.status > 299:
            log.error(http_error_message(r.status, "event delivery", "some events were dropped"))
            if not is_http_error_recoverable(r.status):
                self._disabled = True
                return

    def _do_shutdown(self):
        self._flush_workers.stop()
        self._flush_workers.wait()
        if self._close_http:
            self._http.clear()


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config, http=None):
        self._queue = queue.Queue(config.events_max_pending)
        self._flush_timer = RepeatingTimer(config.flush_interval, self.flush)
        self._users_flush_timer = RepeatingTimer(config.user_keys_flush_interval, self._flush_users)
        self._flush_timer.start()
        self._users_flush_timer.start()
        self._close_lock = Lock()
        self._closed = False
        EventDispatcher(self._queue, config, http)

    def send_event(self, event):
        event['creationDate'] = int(time.time() * 1000)
        self._queue.put(EventProcessorMessage('event', event))

    def flush(self):
        self._queue.put(EventProcessorMessage('flush', None))

    def stop(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
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
