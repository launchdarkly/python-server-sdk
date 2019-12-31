import json
import uuid

from ldclient.config import Config
from ldclient.diagnostics import create_diagnostic_id, create_diagnostic_init, _DiagnosticAccumulator

def test_create_diagnostic_id():
    test_config = Config(sdk_key = "SDK_KEY")
    diag_id = create_diagnostic_id(test_config);
    assert len(diag_id) == 2
    uid = diag_id['diagnosticId']
    # Will throw if invalid UUID4
    uuid.UUID('urn:uuid:' + uid)
    assert diag_id['sdkKeySuffix'] == 'DK_KEY'

def test_create_diagnostic_init():
    test_config = Config(sdk_key = "SDK_KEY", wrapper_name='django', wrapper_version = '5.1.1')
    diag_id = create_diagnostic_id(test_config);
    diag_init = create_diagnostic_init(100, diag_id, test_config)
    assert len(diag_init) == 6
    assert diag_init['kind'] == 'diagnostic-init'
    assert diag_init['id'] == diag_id
    assert diag_init['creationDate'] == 100
    assert diag_init['sdk']
    assert diag_init['platform']
    assert diag_init['configuration']

    # Verify converts to json without failure
    json.dumps(diag_init)

def test_diagnostic_accumulator():
    test_config = Config(sdk_key = "SDK_KEY")
    diag_id = create_diagnostic_id(test_config);
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
