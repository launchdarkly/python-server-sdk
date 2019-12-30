#DEFAULT_CONFIG = Config.default()
#DEFAULT_BASE_URI = DEFAULT_CONFIG.base_uri
#DEFAULT_EVENTS_URI = DEFAULT_CONFIG.events_uri
#DEFAULT_STREAM_BASE_URI = DEFAULT_CONFIG.stream_base_uri
import threading
import time
import uuid

class _DiagnosticAccumulator(object):
    def __init__(self, diagnostic_id):
        self.diagnostic_id = diagnostic_id
        self.data_since_date = int(time.time() * 1000)
        self._state_lock = threading.Lock()
        self._events_in_last_batch = 0
        self._stream_inits = []

    def record_stream_init(self, timestamp, duration, failed):
        with self._state_lock:
            self._stream_inits.append({'timestamp': timestamp,
                                       'durationMillis': duration,
                                       'failed': failed})

    def record_events_in_batch(self, events_in_batch):
        with self._state_lock:
            self._events_in_last_batch = events_in_batch

    def create_event_and_reset(self, dropped_events, deduplicated_users):
        with self._state_lock:
            events_in_batch = self._events_in_last_batch
            stream_inits = self._stream_inits
            self._events_in_last_batch = 0
            self._stream_inits = []

        current_time = int(time.time() * 1000)
        periodic_event = _diagnostic_base_fields('diagnostic', current_time, self.diagnostic_id)
        periodic_event.update({'dataSincedate': self.data_since_date,
                               'droppedEvents': dropped_events,
                               'deduplicatedUsers': deduplicated_users,
                               'eventsInLastBatch': events_in_batch,
                               'streamInits': stream_inits})
        self.data_since_date = current_time
        return periodic_event

def create_diagnostic_id(config):
    return {'diagnosticId': str(uuid.uuid4()),
            'sdkKeySuffix': '' if not config.sdk_key else config.sdk_key[-6:]}

def create_diagnostic_init(creation_date, diagnostic_id, config):
    base_object = _diagnostic_base_fields('diagnostic-init', creation_date, diagnostic_id)
    base_object.update({'configuration': _create_diagnostic_config_object(config),
                        'sdk': _create_diagnostic_sdk_object(config),
                        'platform': _create_diagnostic_platform_object()})
    return base_object

def _diagnostic_base_fields(kind, creation_date, diagnostic_id):
    return {'kind': kind,
            'creationDate': creation_date,
            'id': diagnostic_id}

def _create_diagnostic_config_object(config):
    default_config = config.default()
    return {'customBaseURI': config.base_uri != default_config.base_uri,
            'customEventsURI': config.events_uri != default_config.events_uri,
            'customStreamURI': config.stream_base_uri != default_config.stream_base_uri,
            'eventsCapacity': config.events_max_pending,
            'connectTimeoutMillis': config.connect_timeout * 1000,
            'socketTimeoutMillis': config.read_timeout * 1000,
            'eventsFlushIntervalMillis': config.flush_interval * 1000,
            'usingProxy': config.http_proxy is not None,
            'streamingDisabled': not config.stream,
            'usingRelayDaemon': config.use_ldd,
            'allAttributesPrivate': config.all_attributes_private,
            'pollingIntervalMillis': config.poll_interval * 1000,
            #'reconnectTimeMillis': check,
            'userKeysCapacity': config.user_keys_capacity,
            'userKeysFlushIntervalMillis': config.user_keys_flush_interval * 1000,
            'inlineUsersInEvents': config.inline_users_in_events,
            'diagnosticRecordingIntervalMillis': config.diagnostic_recording_interval * 1000,
            #'featureStoreFactory': check,
            }

def _create_diagnostic_sdk_object(config):
    return {'name': 'python-server-sdk',
            'version': 6, #VERSION,
            'wrapperName': config.wrapper_name,
            'wrapperVersion': config.wrapper_version}

def _create_diagnostic_platform_object():
    return {'name': 'python'}
