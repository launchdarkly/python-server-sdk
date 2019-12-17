DEFAULT_CONFIG = Config('sdk_key')
DEFAULT_BASE_URI = DEFAULT_CONFIG.base_uri
DEFAULT_EVENTS_URI = DEFAULT_CONFIG.events_uri
DEFAULT_STREAM_BASE_URI = DEFAULT_CONFIG.stream_base_uri

def diagnostic_base_fields(kind, creation_date, diagnostic_id):
    return {'kind': kind,
            'creationDate': creation_date,
            'id': diagnostic_id}

def create_diagnostic_statistics(creation_date, diagnostic_id, data_since_date, dropped_events, deduplicated_users, events_in_last_batch):
    base_object = diagnostic_base_fields('diagnostic', creation_date, diagnostic_id)
    base_object.update({'dataSinceDate': data_since_date,
                        'droppedEvents': dropped_events,
                        'deduplicatedUsers': deduplicated_users,
                        'eventsInLastBatch': events_in_last_batch})
    return base_object

def create_diagnostic_config_object(config):
    return {'customBaseURI': config.base_uri != DEFAULT_BASE_URI,
            'customEventsURI': config.events_uri != DEFAULT_EVENTS_URI,
            'customStreamURI': config.stream_base_uri != DEFAULT_STREAM_BASE_URI,
            'eventsCapacity': config.events_max_pending,
            'connectTimeoutMillis': config.connect_timeout * 1000,
            'socketTimeoutMillis': config.read_timeout * 1000,
            'eventsFlushIntervalMillis': config.flush_interval * 1000,
            'usingProxy': False, #TODO
            'usingProxyAuthenticator': False, #TODO
            'streamingDisabled': not config.stream,
            'usingRelayDaemon': False, #TODO
            'offline': config.offline, #Check if this actually makes sense
            'allAttributesPrivate': config.all_attributes_private,
            'pollingIntervalMillis': config.poll_interval * 1000,
            #'startWaitMillis': check,
            #'samplingInterval': check,
            #'reconnectTimeMillis': check,
            'userKeysCapacity': config.user_keys_capacity,
            'userKeysFlushIntervalMillis': config.user_keys_flush_interval * 1000,
            'inlineUsersInEvents': config.inline_users_in_events,
            'diagnosticRecordingIntervalMillis': config.diagnostic_recording_interval * 1000,
            #'featureStoreFactory': check,
            }

def create_diagnostic_sdk_object():
    return {}

def create_diagnostic_platform_object():
    return {}

def create_diagnostic_init(creation_date, diagnostic_id, config):
    base_object = diagnostic_base_fields('diagnostic-init', creation_date, diagnostic_id)
    base_object.update({'configuration': create_diagnostic_config_object(config),
                        'sdk': create_diagnostic_sdk_object(),
                        'platform': create_diagnostic_platform_object()})
    return base_object
