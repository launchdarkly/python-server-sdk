import json

from ldclient.impl.datasourcev2.polling import (
    IntentCode,
    fdv1_polling_payload_to_changeset,
    polling_payload_to_changeset
)
from ldclient.impl.util import _Fail, _Success
from ldclient.interfaces import ChangeType, ObjectKind


def test_payload_is_missing_events_key():
    data = {}
    result = polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert result.error == "Invalid payload: 'events' key is missing or not a list"


def test_payload_events_value_is_invalid():
    data = {"events": "not a list"}
    result = polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert result.error == "Invalid payload: 'events' key is missing or not a list"


def test_payload_event_is_invalid():
    data = {"events": ["this should be a dictionary"]}
    result = polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert result.error == "Invalid payload: 'events' must be a list of objects"


def test_missing_protocol_events():
    data = {"events": []}
    result = polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert result.error == "didn't receive any known protocol events in polling payload"


def test_transfer_none():
    payload_str = '{"events":[{"event": "server-intent","data": {"payloads":[{"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":462,"intentCode":"none","reason":"up-to-date"}]}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))

    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_NONE
    assert len(change_set.changes) == 0
    assert change_set.selector is None


def test_transfer_full_with_empty_payload():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))

    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(change_set.changes) == 0
    assert change_set.selector is not None
    assert change_set.selector.state == "(p:5A46PZ79FQ9D08YYKT79DECDNV:461)"
    assert change_set.selector.version == 461


def test_server_intent_decoding_fails():
    payload_str = '{"events":[ {"event":"server-intent","data":{}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in server intent"
    assert isinstance(result.exception, ValueError)


def test_processes_put_object():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "put-object","data": {"key":"sample-feature","kind":"flag","version":461,"object":{"key":"sample-feature","on":false,"prerequisites":[],"targets":[],"contextTargets":[],"rules":[],"fallthrough":{"variation":0},"offVariation":1,"variations":[true,false],"clientSideAvailability":{"usingMobileKey":false,"usingEnvironmentId":false},"clientSide":false,"salt":"9945e63a79a44787805b79728fee1926","trackEvents":false,"trackEventsFallthrough":false,"debugEventsUntilDate":null,"version":112,"deleted":false}}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(change_set.changes) == 1

    assert change_set.changes[0].action == ChangeType.PUT
    assert change_set.changes[0].kind == ObjectKind.FLAG
    assert change_set.changes[0].key == "sample-feature"
    assert change_set.changes[0].version == 461
    assert isinstance(change_set.changes[0].object, dict)

    assert change_set.selector is not None
    assert change_set.selector.state == "(p:5A46PZ79FQ9D08YYKT79DECDNV:461)"
    assert change_set.selector.version == 461


def test_processes_delete_object():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "delete-object","data": {"key":"sample-feature","kind":"flag","version":461}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(change_set.changes) == 1

    assert change_set.changes[0].action == ChangeType.DELETE
    assert change_set.changes[0].kind == ObjectKind.FLAG
    assert change_set.changes[0].key == "sample-feature"
    assert change_set.changes[0].version == 461
    assert change_set.changes[0].object is None

    assert change_set.selector is not None
    assert change_set.selector.state == "(p:5A46PZ79FQ9D08YYKT79DECDNV:461)"
    assert change_set.selector.version == 461


def test_handles_invalid_put_object():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "put-object","data": {}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in put object"


def test_handles_invalid_delete_object():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "delete-object","data": {}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in delete object"


def test_handles_invalid_payload_transferred():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event":"payload-transferred","data":{}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in payload transferred object"


def test_fails_if_starts_with_transferred():
    payload_str = '{"events":[ {"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}},{"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "put-object","data": {"key":"sample-feature","kind":"flag","version":461,"object":{"key":"sample-feature","on":false,"prerequisites":[],"targets":[],"contextTargets":[],"rules":[],"fallthrough":{"variation":0},"offVariation":1,"variations":[true,false],"clientSideAvailability":{"usingMobileKey":false,"usingEnvironmentId":false},"clientSide":false,"salt":"9945e63a79a44787805b79728fee1926","trackEvents":false,"trackEventsFallthrough":false,"debugEventsUntilDate":null,"version":112,"deleted":false}}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in payload transferred object"
    assert result.exception is not None
    assert (
        result.exception.args[0] == "changeset: cannot complete without a server-intent"
    )


def test_fails_if_starts_with_put():
    payload_str = '{"events":[ {"event": "put-object","data": {"key":"sample-feature","kind":"flag","version":461,"object":{"key":"sample-feature","on":false,"prerequisites":[],"targets":[],"contextTargets":[],"rules":[],"fallthrough":{"variation":0},"offVariation":1,"variations":[true,false],"clientSideAvailability":{"usingMobileKey":false,"usingEnvironmentId":false},"clientSide":false,"salt":"9945e63a79a44787805b79728fee1926","trackEvents":false,"trackEventsFallthrough":false,"debugEventsUntilDate":null,"version":112,"deleted":false}}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}},{"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}}]}'
    result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(result, _Fail)
    assert result.error == "Invalid JSON in payload transferred object"
    assert result.exception is not None
    assert (
        result.exception.args[0] == "changeset: cannot complete without a server-intent"
    )


# FDv1 Payload Parsing Tests
def test_fdv1_payload_empty_flags_and_segments():
    """Test that FDv1 payload with empty flags and segments produces empty changeset."""
    data = {
        "flags": {},
        "segments": {}
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(change_set.changes) == 0
    # FDv1 doesn't use selectors
    assert change_set.selector is not None
    assert not change_set.selector.is_defined()


def test_fdv1_payload_with_single_flag():
    """Test that FDv1 payload with a single flag is parsed correctly."""
    data = {
        "flags": {
            "test-flag": {
                "key": "test-flag",
                "version": 1,
                "on": True,
                "variations": [True, False]
            }
        },
        "segments": {}
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(change_set.changes) == 1

    change = change_set.changes[0]
    assert change.action == ChangeType.PUT
    assert change.kind == ObjectKind.FLAG
    assert change.key == "test-flag"
    assert change.version == 1


def test_fdv1_payload_with_multiple_flags():
    """Test that FDv1 payload with multiple flags is parsed correctly."""
    data = {
        "flags": {
            "flag-1": {"key": "flag-1", "version": 1, "on": True},
            "flag-2": {"key": "flag-2", "version": 2, "on": False},
            "flag-3": {"key": "flag-3", "version": 3, "on": True}
        },
        "segments": {}
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert len(change_set.changes) == 3

    flag_keys = {c.key for c in change_set.changes}
    assert flag_keys == {"flag-1", "flag-2", "flag-3"}


def test_fdv1_payload_with_single_segment():
    """Test that FDv1 payload with a single segment is parsed correctly."""
    data = {
        "flags": {},
        "segments": {
            "test-segment": {
                "key": "test-segment",
                "version": 5,
                "included": ["user1", "user2"]
            }
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert len(change_set.changes) == 1

    change = change_set.changes[0]
    assert change.action == ChangeType.PUT
    assert change.kind == ObjectKind.SEGMENT
    assert change.key == "test-segment"
    assert change.version == 5


def test_fdv1_payload_with_flags_and_segments():
    """Test that FDv1 payload with both flags and segments is parsed correctly."""
    data = {
        "flags": {
            "flag-1": {"key": "flag-1", "version": 1, "on": True},
            "flag-2": {"key": "flag-2", "version": 2, "on": False}
        },
        "segments": {
            "segment-1": {"key": "segment-1", "version": 10},
            "segment-2": {"key": "segment-2", "version": 20}
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert len(change_set.changes) == 4

    flag_changes = [c for c in change_set.changes if c.kind == ObjectKind.FLAG]
    segment_changes = [c for c in change_set.changes if c.kind == ObjectKind.SEGMENT]

    assert len(flag_changes) == 2
    assert len(segment_changes) == 2


def test_fdv1_payload_flags_not_dict():
    """Test that FDv1 payload parser fails when flags namespace is not a dict."""
    data = {
        "flags": "not a dict"
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert "not a dictionary" in result.error


def test_fdv1_payload_segments_not_dict():
    """Test that FDv1 payload parser fails when segments namespace is not a dict."""
    data = {
        "flags": {},
        "segments": "not a dict"
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert "not a dictionary" in result.error


def test_fdv1_payload_flag_value_not_dict():
    """Test that FDv1 payload parser fails when a flag value is not a dict."""
    data = {
        "flags": {
            "bad-flag": "not a dict"
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert "not a dictionary" in result.error


def test_fdv1_payload_flag_missing_version():
    """Test that FDv1 payload parser fails when a flag is missing version."""
    data = {
        "flags": {
            "no-version-flag": {
                "key": "no-version-flag",
                "on": True
            }
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert "does not have a version set" in result.error


def test_fdv1_payload_segment_missing_version():
    """Test that FDv1 payload parser fails when a segment is missing version."""
    data = {
        "flags": {},
        "segments": {
            "no-version-segment": {
                "key": "no-version-segment",
                "included": []
            }
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Fail)
    assert "does not have a version set" in result.error


def test_fdv1_payload_only_flags_no_segments_key():
    """Test that FDv1 payload works when segments key is missing entirely."""
    data = {
        "flags": {
            "test-flag": {"key": "test-flag", "version": 1, "on": True}
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert len(change_set.changes) == 1
    assert change_set.changes[0].key == "test-flag"


def test_fdv1_payload_only_segments_no_flags_key():
    """Test that FDv1 payload works when flags key is missing entirely."""
    data = {
        "segments": {
            "test-segment": {"key": "test-segment", "version": 1}
        }
    }
    result = fdv1_polling_payload_to_changeset(data)
    assert isinstance(result, _Success)

    change_set = result.value
    assert len(change_set.changes) == 1
    assert change_set.changes[0].key == "test-segment"
