# pylint: disable=missing-docstring, too-few-public-methods


import json
from abc import abstractmethod
from typing import Iterable, List, Optional

import pytest
from ld_eventsource.actions import Action, Start
from ld_eventsource.http import HTTPStatusError
from ld_eventsource.sse_client import Event, Fault

from ldclient.config import Config, HTTPConfig
from ldclient.impl.datasourcev2.streaming import (
    STREAMING_ENDPOINT,
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
    def builder(
        base_uri: str,  # pylint: disable=unused-argument
        http_options: HTTPConfig,  # pylint: disable=unused-argument
        initial_reconnect_delay: float,
        config: Config,  # pylint: disable=unused-argument
        ss: SelectorStore  # pylint: disable=unused-argument
    ):
        return ListBasedSseClient(events), None

    return builder


def make_streaming_data_source() -> StreamingDataSource:
    """Helper to create a StreamingDataSource with the new constructor signature."""
    config = Config("key")
    return StreamingDataSource(
        config.stream_base_uri + STREAMING_ENDPOINT,
        config.http,
        config.initial_reconnect_delay,
        config
    )


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
    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = list_sse_client([UnknownTypeOfEvent(), unknown_named_event])

    assert len(list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))) == 0


def test_ignores_faults_without_errors():
    errorless_fault = Fault(error=None)
    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = list_sse_client([intent_event])
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None
    assert updates[0].change_set is None


def test_handles_empty_changeset(events):  # pylint: disable=redefined-outer-name
    builder = list_sse_client(
        [
            events[EventName.SERVER_INTENT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector.is_defined()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1
    assert updates[0].change_set.changes[0].action == ChangeType.PUT
    assert updates[0].change_set.changes[0].kind == ObjectKind.FLAG
    assert updates[0].change_set.changes[0].key == "flag-key"
    assert updates[0].change_set.changes[0].object == {"key": "flag-key"}
    assert updates[0].change_set.changes[0].version == 100
    assert updates[0].change_set.selector.is_defined()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1
    assert updates[0].change_set.changes[0].action == ChangeType.DELETE
    assert updates[0].change_set.changes[0].kind == ObjectKind.FLAG
    assert updates[0].change_set.changes[0].key == "flag-key"
    assert updates[0].change_set.changes[0].version == 101
    assert updates[0].change_set.selector.is_defined()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector.is_defined()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
    assert updates[0].environment_id is None

    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 0
    assert updates[0].change_set.selector.is_defined()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].error is None
    assert updates[0].fallback_to_fdv1 is False
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].change_set is None
    assert updates[0].fallback_to_fdv1 is False
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 2
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].change_set is None
    assert updates[0].fallback_to_fdv1 is False
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].change_set is None
    assert updates[0].fallback_to_fdv1 is False
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
    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id == 'test-env-456'
    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1


def test_fallback_header_with_no_payload_emits_no_update():
    """A Start carrying X-LD-FD-Fallback with no following payload events
    must not synthesize an Update. The directive only fires once a payload
    has been applied or an error has been observed."""
    start_action = Start(headers={_LD_ENVID_HEADER: 'test-env-fallback', _LD_FD_FALLBACK_HEADER: 'true'})

    builder = list_sse_client([start_action])

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert updates == []


def test_fallback_header_with_payload_emits_valid_with_fallback(events):  # pylint: disable=redefined-outer-name
    """When the response carries X-LD-FD-Fallback: true and a valid SSE
    payload, the synchronizer must apply the payload and then emit a single
    Valid update with fallback_to_fdv1=True so the consumer can hand off to
    the FDv1 Fallback Synchronizer."""
    start_action = Start(headers={_LD_ENVID_HEADER: 'test-env-fallback', _LD_FD_FALLBACK_HEADER: 'true'})

    builder = list_sse_client(
        [
            start_action,
            events[EventName.SERVER_INTENT],
            events[EventName.PUT_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]
    )

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].environment_id == 'test-env-fallback'
    assert updates[0].change_set is not None
    assert len(updates[0].change_set.changes) == 1


def test_fallback_latched_on_start_carries_through_unrecoverable_fault():
    """Once a Start latches the FDv1 directive, an unrecoverable Fault that
    follows must propagate the directive even when the error itself does not
    carry the header. The directive is one-way and terminal, so the latched
    state from the original Start drives the Update emitted on shutdown --
    losing it would silently strand the consumer on FDv2 instead of handing
    off to the FDv1 Fallback Synchronizer."""
    start_action = Start(headers={_LD_FD_FALLBACK_HEADER: 'true'})
    # 401 is unrecoverable and the error carries no fallback header itself.
    error = HTTPStatusError(401)
    fault_action = Fault(error=error)

    builder = list_sse_client([start_action, fault_action])

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].error is not None
    assert updates[0].error.status_code == 401


def test_fallback_latched_on_start_carries_through_recoverable_fault():
    """A recoverable Fault arriving after the directive was latched must also
    propagate the signal and halt the stream -- the directive overrides the
    ordinary retry policy because it is terminal."""
    start_action = Start(headers={_LD_FD_FALLBACK_HEADER: 'true'})
    # 408 is recoverable; without the latch we would retry transparently.
    fault_action = Fault(error=HTTPStatusError(408))

    builder = list_sse_client([start_action, fault_action])

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].fallback_to_fdv1 is True
    # 408 is recoverable so the Update from _handle_error is INTERRUPTED, but
    # the latched directive must still drive the consumer to FDv1.
    assert updates[0].state == DataSourceState.INTERRUPTED


def test_fallback_latched_on_start_carries_through_malformed_event(events):  # pylint: disable=redefined-outer-name
    """A malformed event (JSONDecodeError) after the directive was latched
    must propagate the signal on the resulting Interrupted Update."""
    bad_event = Event(event=EventName.PUT_OBJECT, data="not valid json")
    start_action = Start(headers={_LD_FD_FALLBACK_HEADER: 'true'})

    builder = list_sse_client([start_action, events[EventName.SERVER_INTENT], bad_event])

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    # The malformed-event update must surface the latched directive so the
    # consumer can hand off to FDv1 instead of trying to keep the FDv2 stream.
    assert len(updates) == 1
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].error is not None
    assert updates[0].error.kind == DataSourceErrorKind.INVALID_DATA


def test_streaming_closes_underlying_pool_on_fallback(events):  # pylint: disable=redefined-outer-name
    """When the FDv1 Fallback Directive engages, the underlying urllib3
    connection pool must be torn down so the FDv2 streaming TCP connection
    is actually closed. ``SSEClient.close()`` only releases the connection
    back to the pool via a half-close; on Python 3.10 that leaves the socket
    open until GC, which the spec forbids -- the Primary Synchronizer must
    be terminated promptly when the directive fires."""
    pool_close_calls = []

    class TrackingPool:
        """Stand-in PoolManager that records calls to clear() and exposes a
        keys()-iterable pools attribute matching urllib3's RecentlyUsedContainer."""

        def __init__(self):
            self.cleared = False
            self.connection_pool = TrackingConnectionPool()
            self.pools = TrackingPoolDict({"key": self.connection_pool})

        def clear(self):
            self.cleared = True

    class TrackingConnectionPool:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True
            pool_close_calls.append(self)

    class TrackingPoolDict:
        def __init__(self, items):
            self._items = items

        def keys(self):
            return list(self._items.keys())

        def get(self, key):
            return self._items.get(key)

    tracking_pool = TrackingPool()

    def builder(*_args, **_kwargs):
        return ListBasedSseClient([
            Start(headers={_LD_FD_FALLBACK_HEADER: 'true'}),
            events[EventName.SERVER_INTENT],
            events[EventName.PUT_OBJECT],
            events[EventName.PAYLOAD_TRANSFERRED],
        ]), tracking_pool

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder  # type: ignore[assignment]
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].fallback_to_fdv1 is True
    assert tracking_pool.cleared is True
    assert tracking_pool.connection_pool.closed is True


def test_envid_from_fault_action():
    """Test that environment ID is captured from Fault action headers"""
    error = HTTPStatusError(401, headers={_LD_ENVID_HEADER: 'test-env-fault'})
    fault_action = Fault(error=error)

    builder = list_sse_client([fault_action])

    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].fallback_to_fdv1 is True
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

    synchronizer = make_streaming_data_source()
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

    synchronizer = make_streaming_data_source()
    synchronizer._sse_client_builder = builder
    updates = list(synchronizer.sync(MockSelectorStore(Selector.no_selector())))

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
    assert updates[0].environment_id is None
