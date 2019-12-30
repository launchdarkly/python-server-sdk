"""
Implementation details of the analytics event delivery component.
"""
# currently excluded from documentation - see docs/README.md

from collections import namedtuple
from email.utils import parsedate
import errno
import json
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
from ldclient.util import http_error_message, is_http_error_recoverable, stringify_attrs, throw_if_unsuccessful_response
from ldclient.diagnostics import create_diagnostic_init, create_diagnostic_id, _DiagnosticAccumulator

__MAX_FLUSH_THREADS__ = 5
__CURRENT_EVENT_SCHEMA__ = 3
__USER_ATTRS_TO_STRINGIFY_FOR_EVENTS__ = [ "key", "secondary", "ip", "country", "email", "firstName", "lastName", "avatar", "name" ]


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
                out['user'] = self._process_user(e)
            else:
                out['userKey'] = self._get_userkey(e)
            if e.get('reason'):
                out['reason'] = e.get('reason')
            return out
        elif kind == 'identify':
            return {
                'kind': 'identify',
                'creationDate': e['creationDate'],
                'key': self._get_userkey(e),
                'user': self._process_user(e)
            }
        elif kind == 'custom':
            out = {
                'kind': 'custom',
                'creationDate': e['creationDate'],
                'key': e['key']
            }
            if self._inline_users:
                out['user'] = self._process_user(e)
            else:
                out['userKey'] = self._get_userkey(e)
            if e.get('data') is not None:
                out['data'] = e['data']
            if e.get('metricValue') is not None:
                out['metricValue'] = e['metricValue']
            return out
        elif kind == 'index':
            return {
                'kind': 'index',
                'creationDate': e['creationDate'],
                'user': self._process_user(e)
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
    
    def _process_user(self, event):
        filtered = self._user_filter.filter_user_props(event['user'])
        return stringify_attrs(filtered, __USER_ATTRS_TO_STRINGIFY_FOR_EVENTS__)
    
    def _get_userkey(self, event):
        return str(event['user'].get('key'))


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
            json_body = json.dumps(output_events)
            log.debug('Sending events payload: ' + json_body)
            hdrs = _headers(self._config)
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


class DiagnosticEventSendTask(object):
    def __init__(self, http, config, event_body, response_fn):
        self._http = http
        self._config = config
        self._event_body = event_body
        self._response_fn = response_fn

    def run_thread(self):
        try:
            Thread(target = self._do_send()).start()
        except Exception:
            log.warning(
                'Unhandled exception in event processor. Analytics events were not processed.',
                exc_info=True)

    def _do_send(self):
        # noinspection PyBroadException
        try:
            json_body = json.dumps(self._event_body)
            log.debug('Sending diagnostic event: ' + json_body)
            hdrs = _headers(self._config)
            uri = self._config.events_base_uri + '/diagnostic'
            r = self._http.request('POST', uri,
                                   headers=hdrs,
                                   timeout=urllib3.Timeout(connect=self._config.connect_timeout, read=self._config.read_timeout),
                                   body=json_body,
                                   retries=1)
            if (self._response_fn):
                self._response_fn(r)
        except Exception as e:
            log.warning(
                'Unhandled exception in event processor. Diagnostic event was not sent. [%s]', e)


FlushPayload = namedtuple('FlushPayload', ['events', 'summary'])


class EventBuffer(object):
    def __init__(self, capacity):
        self._capacity = capacity
        self._events = []
        self._summarizer = EventSummarizer()
        self._exceeded_capacity = False
        self._dropped_events = 0
    
    def add_event(self, event):
        if len(self._events) >= self._capacity:
            self._dropped_events += 1
            if not self._exceeded_capacity:
                log.warning("Exceeded event queue capacity. Increase capacity to avoid dropping events.")
                self._exceeded_capacity = True
        else:
            self._events.append(event)
            self._exceeded_capacity = False
    
    def add_to_summary(self, event):
        self._summarizer.summarize_event(event)

    def get_and_clear_dropped_count(self):
        dropped_count = self._dropped_events
        self._dropped_events = 0
        return dropped_count

    def get_payload(self):
        return FlushPayload(self._events, self._summarizer.snapshot())
    
    def clear(self):
        self._events = []
        self._summarizer.clear()


class EventDispatcher(object):
    def __init__(self, inbox, config, http_client, diagnostic_accumulator=None):
        self._inbox = inbox
        self._config = config
        self._http = create_http_pool_manager(num_pools=1, verify_ssl=config.verify_ssl,
            target_base_uri=config.events_uri, force_proxy=config.http_proxy) if http_client is None else http_client
        self._close_http = (http_client is None)  # so we know whether to close it later
        self._disabled = False
        self._outbox = EventBuffer(config.events_max_pending)
        self._user_keys = SimpleLRUCache(config.user_keys_capacity)
        self._formatter = EventOutputFormatter(config)
        self._last_known_past_time = 0
        self._deduplicated_users = 0
        self._diagnostic_accumulator = diagnostic_accumulator

        self._flush_workers = FixedThreadPool(__MAX_FLUSH_THREADS__, "ldclient.flush")

        self._main_thread = Thread(target=self._run_main_loop)
        self._main_thread.daemon = True
        self._main_thread.start()

    def _run_main_loop(self):
        log.info("Starting event processor")
        while True:
            try:
                message = self._inbox.get(block=True)
                if message.type == 'event':
                    self._process_event(message.param)
                elif message.type == 'flush':
                    self._trigger_flush()
                elif message.type == 'flush_users':
                    self._user_keys.clear()
                elif message.type == 'diagnostic':
                    self._send_and_reset_diagnostics()
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
        self._outbox.add_to_summary(event)

        # Decide whether to add the event to the payload. Feature events may be added twice, once for
        # the event (if tracked) and once for debugging.
        add_full_event = False
        add_debug_event = False
        add_index_event = False
        if event['kind'] == "feature":
            add_full_event = event.get('trackEvents')
            add_debug_event = self._should_debug_event(event)
        else:
            add_full_event = True

        # For each user we haven't seen before, we add an index event - unless this is already
        # an identify event for that user.
        if not (add_full_event and self._config.inline_users_in_events):
            user = event.get('user')
            if user and 'key' in user:
                is_index_event = event['kind'] == 'identify'
                already_seen = self.notice_user(user)
                add_index_event = not is_index_event and not already_seen
                if not is_index_event and already_seen:
                    self._deduplicated_users += 1

        if add_index_event:
            ie = { 'kind': 'index', 'creationDate': event['creationDate'], 'user': user }
            self._outbox.add_event(ie)
        if add_full_event:
            self._outbox.add_event(event)
        if add_debug_event:
            debug_event = event.copy()
            debug_event['debug'] = True
            self._outbox.add_event(debug_event)

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
        payload = self._outbox.get_payload()
        if self._diagnostic_accumulator:
            self._diagnostic_accumulator.record_events_in_batch(len(payload.events))
        if len(payload.events) > 0 or len(payload.summary.counters) > 0:
            task = EventPayloadSendTask(self._http, self._config, self._formatter, payload,
                self._handle_response)
            if self._flush_workers.execute(task.run):
                # The events have been handed off to a flush worker; clear them from our buffer.
                self._outbox.clear()
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

    def _send_and_reset_diagnostics(self):
        if self._diagnostic_accumulator:
            dropped_event_count = self._outbox.get_and_clear_dropped_count()
            stats_event = self._diagnostic_accumulator.create_event_and_reset(dropped_event_count, self._deduplicated_users)
            self._deduplicated_users = 0
            DiagnosticEventSendTask(self._http, self._config, stats_event, None).run_thread()

    def _do_shutdown(self):
        self._flush_workers.stop()
        self._flush_workers.wait()
        if self._close_http:
            self._http.clear()


class DefaultEventProcessor(EventProcessor):
    def __init__(self, config, http=None, dispatcher_class=None):
        self._inbox = queue.Queue(config.events_max_pending)
        self._inbox_full = False
        self._flush_timer = RepeatingTimer(config.flush_interval, self.flush)
        self._users_flush_timer = RepeatingTimer(config.user_keys_flush_interval, self._flush_users)
        self._flush_timer.start()
        self._users_flush_timer.start()
        self._http = create_http_pool_manager(num_pools=1, verify_ssl=config.verify_ssl,
                                              target_base_uri=config.events_uri,
                                              force_proxy=config.http_proxy) if http is None else http
        if not config.diagnostic_opt_out:
            diagnostic_id = create_diagnostic_id(config)
            self._diagnostic_accumulator = _DiagnosticAccumulator(diagnostic_id)
            init_event = create_diagnostic_init(self._diagnostic_accumulator.data_since_date, diagnostic_id, config)
            DiagnosticEventSendTask(self._http, config, init_event, None).run_thread()

            self._diagnostic_event_timer = RepeatingTimer(config.diagnostic_recording_interval, self._send_diagnostic)
            self._diagnostic_event_timer.start()
        else:
            self._diagnostic_accumulator = None

        self._close_lock = Lock()
        self._closed = False

        (dispatcher_class or EventDispatcher)(self._inbox, config, self._http, self._diagnostic_accumulator)

    def send_event(self, event):
        event['creationDate'] = int(time.time() * 1000)
        self._post_to_inbox(EventProcessorMessage('event', event))

    def flush(self):
        self._post_to_inbox(EventProcessorMessage('flush', None))

    def stop(self):
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self._flush_timer.stop()
        self._users_flush_timer.stop()
        self.flush()
        # Note that here we are not calling _post_to_inbox, because we *do* want to wait if the inbox
        # is full; an orderly shutdown can't happen unless these messages are received.
        self._post_message_and_wait('stop')

    def retrieve_diagnostic_accumulator(self):
        return self._diagnostic_accumulator

    def _post_to_inbox(self, message):
        try:
            self._inbox.put(message, block=False)
        except queue.Full:
            if not self._inbox_full:
                # possible race condition here, but it's of no real consequence - we'd just get an extra log line
                self._inbox_full = True
                log.warning("Events are being produced faster than they can be processed; some events will be dropped")

    def _flush_users(self):
        self._inbox.put(EventProcessorMessage('flush_users', None))

    def _send_diagnostic(self):
        self._inbox.put(EventProcessorMessage('diagnostic', None))

    # Used only in tests
    def _wait_until_inactive(self):
        self._post_message_and_wait('test_sync')

    def _post_message_and_wait(self, type):
        reply = Event()
        self._inbox.put(EventProcessorMessage(type, reply))
        reply.wait()

    # These magic methods allow use of the "with" block in tests
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.stop()
