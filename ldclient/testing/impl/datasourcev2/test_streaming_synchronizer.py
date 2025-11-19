# pylint: disable=missing-docstring, too-few-public-methods


import json
from abc import abstractmethod
from typing import Iterable, List, Optional

import pytest
from ld_eventsource.actions import Action, Start
from ld_eventsource.http import HTTPStatusError
from ld_eventsource.sse_client import Event, Fault

from ldclient.config import Config
from ldclient.impl.datasourcev2.streaming import (
    SSEClient,
    SseClientBuilder,
    StreamingDataSource
)
from ldclient.impl.datasystem.protocolv2 import (
    DeleteObject,
    Error,
    EventName,
    Goodbye,
    PutObject
)
from ldclient.impl.util import _LD_ENVID_HEADER, _LD_FD_FALLBACK_HEADER
from ldclient.interfaces import (
    ChangeType,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Payload,
    Selector,
    SelectorStore,
    ServerIntent
)
from ldclient.testing.mock_components import MockSelectorStore


def list_sse_client(
    events: Iterable[Action],  # pylint: disable=redefined-outer-name
) -> SseClientBuilder:
    def builder(config: Config, ss: SelectorStore) -> SSEClient:
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

    @property
    def next_retry_delay(self):
        return 1

    def interrupt(self):
        pass

    def close(self):
        pass


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
    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = list_sse_client([UnknownTypeOfEvent(), unknown_named_event])

    assert len(list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))) == 0


def test_ignores_faults_without_errors():
    errorless_fault = Fault(error=None)
    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = list_sse_client([errorless_fault])

    assert len(list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))) == 0


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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = list_sse_client([intent_event])
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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
    error = HTTPStatusError(401)
    fault = Fault(error=error)
    builder = list_sse_client(
        [
            # This will generate an error but the stream should continue
            fault,
            # We send these valid combinations to ensure the stream is NOT
            # being processed after the 401.
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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
    error1 = HTTPStatusError(400)
    fault1 = Fault(error=error1)

    error2 = HTTPStatusError(408)
    fault2 = Fault(error=error2)

    builder = list_sse_client(
        [
            # This will generate an error but the stream should continue
            fault1,
            events[EventName.SERVER_INTENT],
            fault2,
            # We send these valid combinations to ensure the stream will
            # continue to be processed.
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )
    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

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


def test_envid_from_start_action(events):  # pylint: disable=redefined-outer-name
    """Test that environment ID is captured from Start action headers"""
    start_action = Start(headers={_LD_ENVID_HEADER: 'test-env-123'})

    builder = list_sse_client(
        [
            start_action,
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id == 'test-env-123'


def test_envid_not_cleared_from_next_start(events):  # pylint: disable=redefined-outer-name
    """Test that environment ID is captured from Start action headers"""
    start_action_with_headers = Start(headers={_LD_ENVID_HEADER: 'test-env-123'})
    start_action_without_headers = Start()

    builder = list_sse_client(
        [
            start_action_with_headers,
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
            start_action_without_headers,
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 2
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id == 'test-env-123'

    assert updates[1].state == DataSourceState.VALID
    assert updates[1].environment_id == 'test-env-123'


def test_envid_preserved_across_events(events):  # pylint: disable=redefined-outer-name
    """Test that environment ID is preserved across multiple events after being set on Start"""
    start_action = Start(headers={_LD_ENVID_HEADER: 'test-env-456'})

    builder = list_sse_client(
        [
            start_action,
            events[EventName.SERVER_INTENT],
            events[EventName.PUT_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id == 'test-env-456'
    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1


def test_envid_from_fallback_header():
    """Test that environment ID is captured when fallback header is present"""
    start_action = Start(headers={_LD_ENVID_HEADER: 'test-env-fallback', _LD_FD_FALLBACK_HEADER: 'true'})

    builder = list_sse_client([start_action])

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].revert_to_fdv1 is True
    assert updates[0].environment_id == 'test-env-fallback'


def test_envid_from_fault_action():
    """Test that environment ID is captured from Fault action headers"""
    error = HTTPStatusError(401, headers={_LD_ENVID_HEADER: 'test-env-fault'})
    fault_action = Fault(error=error)

    builder = list_sse_client([fault_action])

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].environment_id == 'test-env-fault'
    assert updates[0].error is not None
    assert updates[0].error.status_code == 401


def test_envid_not_cleared_from_next_error():
    """Test that environment ID is captured from Fault action headers"""
    error_with_headers_ = HTTPStatusError(408, headers={_LD_ENVID_HEADER: 'test-env-fault'})
    error_without_headers_ = HTTPStatusError(401)
    fault_action_with_headers = Fault(error=error_with_headers_)
    fault_action_without_headers = Fault(error=error_without_headers_)

    builder = list_sse_client([fault_action_with_headers, fault_action_without_headers])

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 2
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].environment_id == 'test-env-fault'
    assert updates[0].error is not None
    assert updates[0].error.status_code == 408

    assert updates[1].state == DataSourceState.OFF
    assert updates[1].environment_id == 'test-env-fault'
    assert updates[1].error is not None
    assert updates[1].error.status_code == 401


def test_envid_from_fault_with_fallback():
    """Test that environment ID and fallback are captured from Fault action"""
    error = HTTPStatusError(503, headers={_LD_ENVID_HEADER: 'test-env-503', _LD_FD_FALLBACK_HEADER: 'true'})
    fault_action = Fault(error=error)

    builder = list_sse_client([fault_action])

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].revert_to_fdv1 is True
    assert updates[0].environment_id == 'test-env-503'


def test_envid_from_recoverable_fault(events):  # pylint: disable=redefined-outer-name
    """Test that environment ID is captured from recoverable Fault and preserved in subsequent events"""
    error = HTTPStatusError(400, headers={_LD_ENVID_HEADER: 'test-env-400'})
    fault_action = Fault(error=error)

    builder = list_sse_client(
        [
            fault_action,
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 2
    # First update from the fault
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].environment_id == 'test-env-400'

    # Second update should preserve the envid
    assert updates[1].state == DataSourceState.VALID
    assert updates[1].environment_id == 'test-env-400'


def test_envid_missing_when_no_headers():
    """Test that environment ID is None when no headers are present"""
    start_action = Start()

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

    builder = list_sse_client([start_action, intent_event])

    synchronizer = StreamingDataSource(Config(sdk_key="key"))
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id is None
