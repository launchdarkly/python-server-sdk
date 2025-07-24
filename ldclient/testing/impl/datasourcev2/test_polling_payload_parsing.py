import json

from ldclient.impl.datasourcev2.polling import (
    IntentCode,
    polling_payload_to_changeset
)
from ldclient.impl.datasystem.protocolv2 import ChangeType, ObjectKind
from ldclient.impl.util import _Fail, _Success


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
