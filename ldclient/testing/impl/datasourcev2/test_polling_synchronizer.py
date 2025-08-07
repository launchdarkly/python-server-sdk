import json
from typing import Iterator, Optional

import pytest
from ld_eventsource.sse_client import Event

from ldclient.impl.datasourcev2 import PollingResult
from ldclient.impl.datasourcev2.polling import PollingDataSource
from ldclient.impl.datasystem.protocolv2 import (
    ChangeSetBuilder,
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
from ldclient.impl.util import UnsuccessfulResponseException, _Fail, _Success
from ldclient.interfaces import DataSourceErrorKind, DataSourceState


class ListBasedRequester:
    def __init__(self, results: Iterator[PollingResult]):
        self._results = results
        self._index = 0

    def fetch(
        self, selector: Optional[Selector]
    ) -> PollingResult:  # pylint: disable=unused-argument
        return next(self._results)


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
    change_set = ChangeSetBuilder.no_changes()
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    synchronizer = PollingDataSource(
        poll_interval=0.01, requester=ListBasedRequester(results=iter([polling_result]))
    )

    valid = next(synchronizer.sync())

    assert valid.state == DataSourceState.VALID
    assert valid.error is None
    assert valid.revert_to_fdv1 is False
    assert valid.environment_id is None
    assert valid.change_set is not None
    assert valid.change_set.intent_code == IntentCode.TRANSFER_NONE
    assert len(valid.change_set.changes) == 0


def test_handles_empty_changeset():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    synchronizer = PollingDataSource(
        poll_interval=0.01, requester=ListBasedRequester(results=iter([polling_result]))
    )
    valid = next(synchronizer.sync())

    assert valid.state == DataSourceState.VALID
    assert valid.error is None
    assert valid.revert_to_fdv1 is False
    assert valid.environment_id is None

    assert valid.change_set is not None
    assert len(valid.change_set.changes) == 0
    assert valid.change_set.selector is not None
    assert valid.change_set.selector.version == 300
    assert valid.change_set.selector.state == "p:SOMETHING:300"
    assert valid.change_set.intent_code == IntentCode.TRANSFER_FULL


def test_handles_put_objects():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    builder.add_put(
        version=100, kind=ObjectKind.FLAG, key="flag-key", obj={"key": "flag-key"}
    )
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    synchronizer = PollingDataSource(
        poll_interval=0.01, requester=ListBasedRequester(results=iter([polling_result]))
    )
    valid = next(synchronizer.sync())

    assert valid.state == DataSourceState.VALID
    assert valid.error is None
    assert valid.revert_to_fdv1 is False
    assert valid.environment_id is None

    assert valid.change_set is not None
    assert len(valid.change_set.changes) == 1
    assert valid.change_set.changes[0].action == ChangeType.PUT
    assert valid.change_set.changes[0].kind == ObjectKind.FLAG
    assert valid.change_set.changes[0].key == "flag-key"
    assert valid.change_set.changes[0].object == {"key": "flag-key"}
    assert valid.change_set.changes[0].version == 100
    assert valid.change_set.selector is not None
    assert valid.change_set.selector.version == 300
    assert valid.change_set.selector.state == "p:SOMETHING:300"
    assert valid.change_set.intent_code == IntentCode.TRANSFER_FULL


def test_handles_delete_objects():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    builder.add_delete(version=101, kind=ObjectKind.FLAG, key="flag-key")
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    synchronizer = PollingDataSource(
        poll_interval=0.01, requester=ListBasedRequester(results=iter([polling_result]))
    )
    valid = next(synchronizer.sync())

    assert valid.state == DataSourceState.VALID
    assert valid.error is None
    assert valid.revert_to_fdv1 is False
    assert valid.environment_id is None

    assert valid.change_set is not None
    assert len(valid.change_set.changes) == 1
    assert valid.change_set.changes[0].action == ChangeType.DELETE
    assert valid.change_set.changes[0].kind == ObjectKind.FLAG
    assert valid.change_set.changes[0].key == "flag-key"
    assert valid.change_set.changes[0].version == 101
    assert valid.change_set.selector is not None
    assert valid.change_set.selector.version == 300
    assert valid.change_set.selector.state == "p:SOMETHING:300"
    assert valid.change_set.intent_code == IntentCode.TRANSFER_FULL


# def test_swallows_goodbye(events):  # pylint: disable=redefined-outer-name
#     builder = list_sse_client(
#         [
#             events[EventName.SERVER_INTENT],
#             events[EventName.GOODBYE],
#             events[EventName.PAYLOAD_TRANSFERRED],
#         ]
#     )
#
#     synchronizer = StreamingSynchronizer(Config(sdk_key="key"), builder)
#     updates = list(synchronizer.sync())
#
#     builder = ChangeSetBuilder()
#     builder.start(intent=IntentCode.TRANSFER_FULL)
#     change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
#     headers = {}
#     polling_result: PollingResult = _Success(value=(change_set, headers))
#
#     synchronizer = PollingDataSource(
#         poll_interval=0.01, requester=ListBasedRequester(results=iter([polling_result]))
#     )
#     updates = list(synchronizer.sync())
#
#     assert len(updates) == 1
#     assert updates[0].state == DataSourceState.VALID
#     assert updates[0].error is None
#     assert updates[0].revert_to_fdv1 is False
#     assert updates[0].environment_id is None
#
#     assert updates[0].change_set is not None
#     assert len(updates[0].change_set.changes) == 1
#     assert updates[0].change_set.changes[0].action == ChangeType.DELETE
#     assert updates[0].change_set.changes[0].kind == ObjectKind.FLAG
#     assert updates[0].change_set.changes[0].key == "flag-key"
#     assert updates[0].change_set.changes[0].version == 101
#     assert updates[0].change_set.selector is not None
#     assert updates[0].change_set.selector.version == 300
#     assert updates[0].change_set.selector.state == "p:SOMETHING:300"
#     assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL
#
#     assert len(updates) == 1
#     assert updates[0].state == DataSourceState.VALID
#     assert updates[0].error is None
#     assert updates[0].revert_to_fdv1 is False
#     assert updates[0].environment_id is None
#
#     assert updates[0].change_set is not None
#     assert len(updates[0].change_set.changes) == 0
#     assert updates[0].change_set.selector is not None
#     assert updates[0].change_set.selector.version == 300
#     assert updates[0].change_set.selector.state == "p:SOMETHING:300"
#     assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL
#
#
# def test_swallows_heartbeat(events):  # pylint: disable=redefined-outer-name
#     builder = list_sse_client(
#         [
#             events[EventName.SERVER_INTENT],
#             events[EventName.HEARTBEAT],
#             events[EventName.PAYLOAD_TRANSFERRED],
#         ]
#     )
#
#     synchronizer = StreamingSynchronizer(Config(sdk_key="key"), builder)
#     updates = list(synchronizer.sync())
#
#     assert len(updates) == 1
#     assert updates[0].state == DataSourceState.VALID
#     assert updates[0].error is None
#     assert updates[0].revert_to_fdv1 is False
#     assert updates[0].environment_id is None
#
#     assert updates[0].change_set is not None
#     assert len(updates[0].change_set.changes) == 0
#     assert updates[0].change_set.selector is not None
#     assert updates[0].change_set.selector.version == 300
#     assert updates[0].change_set.selector.state == "p:SOMETHING:300"
#     assert updates[0].change_set.intent_code == IntentCode.TRANSFER_FULL
#
#
def test_generic_error_interrupts_and_recovers():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    builder.add_delete(version=101, kind=ObjectKind.FLAG, key="flag-key")
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    synchronizer = PollingDataSource(
        poll_interval=0.01,
        requester=ListBasedRequester(
            results=iter([_Fail(error="error for test"), polling_result])
        ),
    )
    sync = synchronizer.sync()
    interrupted = next(sync)
    valid = next(sync)

    assert interrupted.state == DataSourceState.INTERRUPTED
    assert interrupted.error is not None
    assert interrupted.error.kind == DataSourceErrorKind.NETWORK_ERROR
    assert interrupted.error.status_code == 0
    assert interrupted.error.message == "error for test"
    assert interrupted.revert_to_fdv1 is False
    assert interrupted.environment_id is None

    assert valid.change_set is not None
    assert len(valid.change_set.changes) == 1
    assert valid.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert valid.change_set.changes[0].action == ChangeType.DELETE


def test_recoverable_error_continues():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    builder.add_delete(version=101, kind=ObjectKind.FLAG, key="flag-key")
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    _failure = _Fail(
        error="error for test", exception=UnsuccessfulResponseException(status=408)
    )

    synchronizer = PollingDataSource(
        poll_interval=0.01,
        requester=ListBasedRequester(results=iter([_failure, polling_result])),
    )
    sync = synchronizer.sync()
    interrupted = next(sync)
    valid = next(sync)

    assert interrupted.state == DataSourceState.INTERRUPTED
    assert interrupted.error is not None
    assert interrupted.error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert interrupted.error.status_code == 408
    assert interrupted.revert_to_fdv1 is False
    assert interrupted.environment_id is None

    assert valid.state == DataSourceState.VALID
    assert valid.error is None
    assert valid.revert_to_fdv1 is False
    assert valid.environment_id is None

    assert valid.change_set is not None
    assert len(valid.change_set.changes) == 1
    assert valid.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert valid.change_set.changes[0].action == ChangeType.DELETE


def test_unrecoverable_error_shuts_down():
    builder = ChangeSetBuilder()
    builder.start(intent=IntentCode.TRANSFER_FULL)
    builder.add_delete(version=101, kind=ObjectKind.FLAG, key="flag-key")
    change_set = builder.finish(selector=Selector(state="p:SOMETHING:300", version=300))
    headers = {}
    polling_result: PollingResult = _Success(value=(change_set, headers))

    _failure = _Fail(
        error="error for test", exception=UnsuccessfulResponseException(status=401)
    )

    synchronizer = PollingDataSource(
        poll_interval=0.01,
        requester=ListBasedRequester(results=iter([_failure, polling_result])),
    )
    sync = synchronizer.sync()
    off = next(sync)
    assert off.state == DataSourceState.OFF
    assert off.error is not None
    assert off.error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert off.error.status_code == 401
    assert off.revert_to_fdv1 is False
    assert off.environment_id is None
    assert off.change_set is None

    try:
        next(sync)
        assert False, "Expected StopIteration"
    except StopIteration:
        pass
