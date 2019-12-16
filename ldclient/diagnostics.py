DEFAULT_CONFIG = Config('sdk_key')
DEFAULT_BASE_URI = DEFAULT_CONFIG.base_uri
DEFAULT_EVENTS_URI = DEFAULT_CONFIG.events_uri
DEFAULT_STREAM_BASE_URI = DEFAULT_CONFIG.stream_base_uri

def create_diagnostic_config_object(config):
    return {'customBaseURI': False if config.base_uri == DEFAULT_BASE_URI else True,
            'customEventsURI': False if config.events_uri == DEFAULT_EVENTS_URI else True,
            'customStreamURI': False if config.stream_base_uri == DEFAULT_STREAM_BASE_URI else True,
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
