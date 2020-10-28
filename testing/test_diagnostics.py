import json
import uuid

from ldclient.config import Config, HTTPConfig
from ldclient.diagnostics import create_diagnostic_id, create_diagnostic_init, _DiagnosticAccumulator, _create_diagnostic_config_object
from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper

def test_create_diagnostic_id():
    test_config = Config(sdk_key = "SDK_KEY", http=HTTPConfig())
    diag_id = create_diagnostic_id(test_config)
    assert len(diag_id) == 2
    uid = diag_id['diagnosticId']
    # Will throw if invalid UUID4
    uuid.UUID('urn:uuid:' + uid)
    assert diag_id['sdkKeySuffix'] == 'DK_KEY'

def test_create_diagnostic_init():
    test_config = Config(sdk_key = "SDK_KEY", wrapper_name='django', wrapper_version = '5.1.1')
    diag_id = create_diagnostic_id(test_config)
    diag_init = create_diagnostic_init(100, diag_id, test_config)
    assert len(diag_init) == 6
    assert diag_init['kind'] == 'diagnostic-init'
    assert diag_init['id'] == diag_id
    assert diag_init['creationDate'] == 100

    assert diag_init['sdk']['name'] == 'python-server-sdk'
    assert diag_init['sdk']['version']
    assert diag_init['sdk']['wrapperName'] == 'django'
    assert diag_init['sdk']['wrapperVersion'] == '5.1.1'

    assert len(diag_init['platform']) == 6
    assert diag_init['platform']['name'] == 'python'
    assert all(x in diag_init['platform'].keys() for x in ['osArch', 'osName', 'osVersion', 'pythonVersion', 'pythonImplementation'])

    assert diag_init['configuration'] == _create_diagnostic_config_object(test_config)

    # Verify converts to json without failure
    json.dumps(diag_init)

def test_create_diagnostic_config_defaults():
    test_config = Config("SDK_KEY")
    diag_config = _create_diagnostic_config_object(test_config)

    assert len(diag_config) == 17
    assert diag_config['customBaseURI'] is False
    assert diag_config['customEventsURI'] is False
    assert diag_config['customStreamURI'] is False
    assert diag_config['eventsCapacity'] == 10000
    assert diag_config['connectTimeoutMillis'] == 10000
    assert diag_config['socketTimeoutMillis'] == 15000
    assert diag_config['eventsFlushIntervalMillis'] == 5000
    assert diag_config['usingProxy'] is False
    assert diag_config['streamingDisabled'] is False
    assert diag_config['usingRelayDaemon'] is False
    assert diag_config['allAttributesPrivate'] is False
    assert diag_config['pollingIntervalMillis'] == 30000
    assert diag_config['userKeysCapacity'] == 1000
    assert diag_config['userKeysFlushIntervalMillis'] == 300000
    assert diag_config['inlineUsersInEvents'] is False
    assert diag_config['diagnosticRecordingIntervalMillis'] == 900000
    assert diag_config['dataStoreType'] == 'memory'

def test_create_diagnostic_config_custom():
    test_store = CachingStoreWrapper(_TestStoreForDiagnostics(), CacheConfig.default())
    test_config = Config("SDK_KEY", base_uri='https://test.com', events_uri='https://test.com',
                         events_max_pending=10, flush_interval=1, stream_uri='https://test.com',
                         stream=False, poll_interval=60, use_ldd=True, feature_store=test_store,
                         all_attributes_private=True, user_keys_capacity=10, user_keys_flush_interval=60,
                         inline_users_in_events=True, http=HTTPConfig(http_proxy = 'proxy', read_timeout=1, connect_timeout=1), diagnostic_recording_interval=60)
    diag_config = _create_diagnostic_config_object(test_config)

    assert len(diag_config) == 17
    assert diag_config['customBaseURI'] is True
    assert diag_config['customEventsURI'] is True
    assert diag_config['customStreamURI'] is True
    assert diag_config['eventsCapacity'] == 10
    assert diag_config['connectTimeoutMillis'] == 1000
    assert diag_config['socketTimeoutMillis'] == 1000
    assert diag_config['eventsFlushIntervalMillis'] == 1000
    assert diag_config['usingProxy'] is True
    assert diag_config['streamingDisabled'] is True
    assert diag_config['usingRelayDaemon'] is True
    assert diag_config['allAttributesPrivate'] is True
    assert diag_config['pollingIntervalMillis'] == 60000
    assert diag_config['userKeysCapacity'] == 10
    assert diag_config['userKeysFlushIntervalMillis'] == 60000
    assert diag_config['inlineUsersInEvents'] is True
    assert diag_config['diagnosticRecordingIntervalMillis'] == 60000
    assert diag_config['dataStoreType'] == 'MyFavoriteStore'

class _TestStoreForDiagnostics:
    def describe_configuration(self, config):
        return 'MyFavoriteStore'

def test_diagnostic_accumulator():
    test_config = Config(sdk_key = "SDK_KEY")
    diag_id = create_diagnostic_id(test_config)
    diag_accum = _DiagnosticAccumulator(diag_id)

    # Test default periodic event
    def_diag_event = diag_accum.create_event_and_reset(0, 0)
    assert len(def_diag_event) == 8
    assert def_diag_event['kind'] == 'diagnostic'
    assert def_diag_event['id'] == diag_id
    assert def_diag_event['creationDate'] == diag_accum.data_since_date
    assert def_diag_event['dataSinceDate']
    assert def_diag_event['droppedEvents'] == 0
    assert def_diag_event['deduplicatedUsers'] == 0
    assert def_diag_event['eventsInLastBatch'] == 0
    assert def_diag_event['streamInits'] == []

    # Verify converts to json without failure
    json.dumps(def_diag_event)

    # Test periodic event after recording values
    diag_accum.record_stream_init(100, 100, False)
    diag_accum.record_stream_init(300, 200, True)
    diag_accum.record_events_in_batch(10)
    diag_accum.record_events_in_batch(50)
    diag_event = diag_accum.create_event_and_reset(10, 15)
    assert len(diag_event) == 8
    assert diag_event['kind'] == 'diagnostic'
    assert diag_event['id'] == diag_id
    assert diag_event['creationDate'] == diag_accum.data_since_date
    assert diag_event['dataSinceDate'] == def_diag_event['creationDate']
    assert diag_event['droppedEvents'] == 10
    assert diag_event['deduplicatedUsers'] == 15
    assert diag_event['eventsInLastBatch'] == 50
    assert diag_event['streamInits'] == [{'timestamp': 100, 'durationMillis': 100, 'failed': False},
                                         {'timestamp': 300, 'durationMillis': 200, 'failed': True}]
    json.dumps(diag_event)

    reset_diag_event = diag_accum.create_event_and_reset(0, 0)
    assert reset_diag_event['creationDate'] == diag_accum.data_since_date
    assert reset_diag_event['dataSinceDate'] == diag_event['creationDate']
    del reset_diag_event['creationDate']
    del def_diag_event['creationDate']
    del reset_diag_event['dataSinceDate']
    del def_diag_event['dataSinceDate']
    assert reset_diag_event == def_diag_event
