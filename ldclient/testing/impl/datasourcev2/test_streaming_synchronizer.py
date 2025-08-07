# pylint: disable=missing-docstring, too-few-public-methods


import json
from abc import abstractmethod
from typing import Iterable, List, Optional

import pytest
from ld_eventsource.actions import Action
from ld_eventsource.http import HTTPStatusError
from ld_eventsource.sse_client import Event, Fault

from ldclient.config import Config
from ldclient.impl.datasourcev2.streaming import (
    SSEClient,
    SseClientBuilder,
    StreamingDataSource
)
from ldclient.impl.datasystem.protocolv2 import (
    ChangeType,
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    IntentCode,
    ObjectKind,
    Payload,
    PutObject,
    Selector,
    ServerIntent
)
from ldclient.interfaces import DataSourceErrorKind, DataSourceState


def list_sse_client(
    events: Iterable[Action],  # pylint: disable=redefined-outer-name
) -> SseClientBuilder:
    def builder(_: Config) -> SSEClient:
        return ListBasedSseClient(events)

    return builder


class ListBasedSseClient:
    def __init__(
        self, events: Optional[Iterable[Action]] = None
    ):  # pylint: disable=redefined-outer-name
        self._events = [] if events is None else events

    @property
    def all(self) -> Iterable[Action]:
        return self._events


class HttpExceptionThrowingSseClient:
    def __init__(self, status_codes: List[int]):  # pylint: disable=redefined-outer-name
        self._status_codes = status_codes
        self._index = 0

    @property
    @abstractmethod
    def all(self) -> Iterable[Action]:
        if self._index >= len(self._status_codes):
            raise IndexError("Invalid number of status codes provided")

        code = self._status_codes[self._index % len(self._status_codes)]
        self._index += 1

        raise HTTPStatusError(code)


def test_ignores_unknown_events():
    class UnknownTypeOfEvent(Action):
        pass

    unknown_named_event = Event(event="Unknown")
    builder = list_sse_client([UnknownTypeOfEvent(), unknown_named_event])
    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)

    assert len(list(synchronizer.sync())) == 0


def test_ignores_faults_without_errors():
    errorless_fault = Fault(error=None)
    builder = list_sse_client([errorless_fault])
    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)

    assert len(list(synchronizer.sync())) == 0


@pytest.fixture
def events() -> dict:
    server_intent = ServerIntent(
        payload=Payload(
            id="id",
            target=300,
            code=IntentCode.TRANSFER_FULL,
            reason="cant-catchup",
        )
    )
    intent_event = Event(
        event=EventName.SERVER_INTENT,
        data=json.dumps(server_intent.to_dict()),
    )

    put = PutObject(
        version=100, kind=ObjectKind.FLAG, key="flag-key", object={"key": "flag-key"}
    )
    put_event = Event(
        event=EventName.PUT_OBJECT,
        data=json.dumps(put.to_dict()),
    )
    delete = DeleteObject(version=101, kind=ObjectKind.FLAG, key="flag-key")
    delete_event = Event(
        event=EventName.DELETE_OBJECT,
        data=json.dumps(delete.to_dict()),
    )

    selector = Selector(state="p:SOMETHING:300", version=300)
    payload_transferred_event = Event(
        event=EventName.PAYLOAD_TRANSFERRED,
        data=json.dumps(selector.to_dict()),
    )

    goodbye = Goodbye(reason="test reason", silent=True, catastrophe=False)
    goodbye_event = Event(
        event=EventName.GOODBYE,
        data=json.dumps(goodbye.to_dict()),
    )

    error = Error(payload_id="p:SOMETHING:300", reason="test reason")
    error_event = Event(
        event=EventName.ERROR,
        data=json.dumps(error.to_dict()),
    )

    heartbeat_event = Event(event=EventName.HEARTBEAT)

    return {
        EventName.SERVER_INTENT: intent_event,
        EventName.PAYLOAD_TRANSFERRED: payload_transferred_event,
        EventName.PUT_OBJECT: put_event,
        EventName.DELETE_OBJECT: delete_event,
        EventName.GOODBYE: goodbye_event,
        EventName.ERROR: error_event,
        EventName.HEARTBEAT: heartbeat_event,
    }


def test_handles_no_changes():
    server_intent = ServerIntent(
        payload=Payload(
            id="id",
            target=300,
            code=IntentCode.TRANSFER_NONE,
            reason="up-to-date",
        )
    )
    intent_event = Event(
        event=EventName.SERVER_INTENT,
        data=json.dumps(server_intent.to_dict()),
    )
    builder = list_sse_client([intent_event])

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None
    assert updates[0].change_set is None


def test_handles_empty_changeset(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector is not None
    assert updates[0].change_set.selector.version == 300
    assert updates[0].change_set.selector.state == "p:SOMETHING:300"
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL


def test_handles_put_objects(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.PUT_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1
    assert updates[0].change_set.changes[0].action == ChangeType.PUT
    assert updates[0].change_set.changes[0].kind == ObjectKind.FLAG
    assert updates[0].change_set.changes[0].key == "flag-key"
    assert updates[0].change_set.changes[0].object == {"key": "flag-key"}
    assert updates[0].change_set.changes[0].version == 100
    assert updates[0].change_set.selector is not None
    assert updates[0].change_set.selector.version == 300
    assert updates[0].change_set.selector.state == "p:SOMETHING:300"
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL


def test_handles_delete_objects(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.DELETE_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1
    assert updates[0].change_set.changes[0].action == ChangeType.DELETE
    assert updates[0].change_set.changes[0].kind == ObjectKind.FLAG
    assert updates[0].change_set.changes[0].key == "flag-key"
    assert updates[0].change_set.changes[0].version == 101
    assert updates[0].change_set.selector is not None
    assert updates[0].change_set.selector.version == 300
    assert updates[0].change_set.selector.state == "p:SOMETHING:300"
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL


def test_swallows_goodbye(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.GOODBYE],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector is not None
    assert updates[0].change_set.selector.version == 300
    assert updates[0].change_set.selector.state == "p:SOMETHING:300"
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL


def test_swallows_heartbeat(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.HEARTBEAT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector is not None
    assert updates[0].change_set.selector.version == 300
    assert updates[0].change_set.selector.state == "p:SOMETHING:300"
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL


def test_error_resets(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.PUT_OBJECT],
            events[EventName.ERROR],
            events[EventName.DELETE_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1
    assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL
    assert updates[0].change_set.changes[0].action == ChangeType.DELETE


def test_handles_out_of_order(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.PUT_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].change_set is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].error is not None
    assert updates[0].error.kind == DataSourceErrorKind.UNKNOWN
    assert updates[0].error.status_code == 0


def test_invalid_json_decoding(events):  # pylint: disable=redefined-outer-name
    intent_event = Event(
        event=EventName.SERVER_INTENT,
        data="{invalid_json",
    )
    builder = list_sse_client(
        [
            # This will generate an error but the stream should continue
            intent_event,
            # We send these valid combinations to ensure we get the stream back
            # on track.
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 2
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].change_set is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].error is not None
    assert updates[0].error.kind == DataSourceErrorKind.INVALID_DATA
    assert updates[0].error.status_code == 0

    assert updates[1].state == DataSourceState.VALID
    assert updates[1].change_set is not None
    assert len(updates[1].change_set.changes) == 0


def test_stops_on_unrecoverable_status_code(
    events,
):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            # This will generate an error but the stream should continue
            Fault(error=HTTPStatusError(401)),
            # We send these valid combinations to ensure the stream is NOT
            # being processed after the 401.
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].change_set is None
    assert updates[0].revert_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].error is not None
    assert updates[0].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert updates[0].error.status_code == 401


def test_continues_on_recoverable_status_code(
    events,
):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            # This will generate an error but the stream should continue
            Fault(error=HTTPStatusError(400)),
            events[EventName.SERVER_INTENT],
            Fault(error=HTTPStatusError(408)),
            # We send these valid combinations to ensure the stream will
            # continue to be processed.
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )
    synchronizer = StreamingDataSource(Config(sdk_key="key"), builder)
    updates = list(synchronizer.sync())

    assert len(updates) == 3
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].error is not None
    assert updates[0].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert updates[0].error.status_code == 400

    assert updates[1].state == DataSourceState.INTERRUPTED
    assert updates[1].error is not None
    assert updates[1].error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert updates[1].error.status_code == 408

    assert updates[2].state == DataSourceState.VALID
    assert updates[2].change_set is not None
    assert len(updates[2].change_set.changes) == 0
    assert updates[2].change_set.selector.version == 300
    assert updates[2].change_set.selector.state == "p:SOMETHING:300"
    assert updates[2].change_set.intent_code == IntentCode.TRANSFER_FULL
