import time
from queue import Queue
from threading import Event
from typing import Any, Callable, List, Tuple

from ldclient import datasystem
from ldclient.client import Config, LDClient
from ldclient.config import BigSegmentsConfig
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import DataSourceState, DataStoreMode
from ldclient.testing.http_util import start_server
from ldclient.testing.mock_components import MockBigSegmentStore
from ldclient.testing.stub_util import (
    MockEventProcessor,
    MockUpdateProcessor,
    make_put_event,
    stream_content
)


def test_big_segment_store_status_unavailable():
    config = Config(sdk_key='SDK_KEY', event_processor_class=MockEventProcessor, update_processor_class=MockUpdateProcessor)
    client = LDClient(config)
    assert client.big_segment_store_status_provider.status.available is False


def test_big_segment_store_status_updates():
    segstore = MockBigSegmentStore()
    segstore.setup_metadata_always_up_to_date()
    config = Config(sdk_key='SDK_KEY', big_segments=BigSegmentsConfig(store=segstore, status_poll_interval=0.01), event_processor_class=MockEventProcessor, update_processor_class=MockUpdateProcessor)
    statuses = Queue()

    with LDClient(config) as client:
        client.big_segment_store_status_provider.add_listener(lambda status: statuses.put(status))

        status1 = client.big_segment_store_status_provider.status
        assert status1.available is True
        assert status1.stale is False

        segstore.setup_metadata_always_stale()

        status2 = statuses.get(True, 1.0)
        assert status2.available is True
        assert status2.stale is True

        segstore.setup_metadata_always_up_to_date()

        status3 = statuses.get(True, 1.0)
        assert status3.available is True
        assert status3.stale is False
        assert client.big_segment_store_status_provider.status.available is True


def test_data_source_status_default():
    config = Config(sdk_key='SDK_KEY', event_processor_class=MockEventProcessor, update_processor_class=MockUpdateProcessor)
    client = LDClient(config)
    assert client.data_source_status_provider.status.state == DataSourceState.INITIALIZING


def test_data_source_status_updates():
    with start_server() as stream_server:
        with stream_content(make_put_event()) as stream_handler:
            stream_server.for_path('/all', stream_handler)
            config = Config(sdk_key='sdk-key', stream_uri=stream_server.uri, send_events=False)

            with LDClient(config=config) as client:
                assert client.data_source_status_provider.status.state == DataSourceState.VALID
                assert client.data_source_status_provider.status.error is None


def test_client_can_be_closed_from_a_flag_change_listener():
    td = TestDataV2.data_source()
    td.update(td.flag('flag').on(True))
    store = _CloseTrackingStore()
    client = _make_client(td, store)
    try:
        closed, errors = _close_from_listener(client, client.flag_tracker.add_listener)
        td.update(td.flag('flag').on(False))
        _assert_closed_from_listener(client, store, closed, errors)
    finally:
        client.close()


def test_client_can_be_closed_from_a_data_store_status_listener():
    td = TestDataV2.data_source()
    td.update(td.flag('flag').on(True))
    store = _CloseTrackingStore()
    client = _make_client(td, store)
    try:
        closed, errors = _close_from_listener(client, client.data_store_status_provider.add_listener)
        # A failed write reports the store as unavailable on the thread that applies the update.
        store.fail_writes = True
        td.update(td.flag('flag').on(False))
        _assert_closed_from_listener(client, store, closed, errors)
    finally:
        client.close()


def _make_client(td: TestDataV2, store: '_CloseTrackingStore') -> LDClient:
    data_system = datasystem.custom().synchronizers(td.builder).data_store(store, DataStoreMode.READ_WRITE).build()
    client = LDClient(Config('SDK_KEY', send_events=False, datasystem_config=data_system), start_wait=5)
    assert client.is_initialized()
    return client


def _close_from_listener(client: LDClient, add_listener: Callable[[Callable[[Any], None]], None]) -> Tuple[Event, List[Exception]]:
    closed = Event()
    errors: List[Exception] = []

    def listener(_):
        if closed.is_set():
            return
        try:
            client.close()
        except Exception as e:
            errors.append(e)
        finally:
            closed.set()

    add_listener(listener)
    return closed, errors


def _assert_closed_from_listener(client: LDClient, store: '_CloseTrackingStore', closed: Event, errors: List[Exception]):
    assert closed.wait(5), "Listener was not called in time"
    assert errors == []
    assert store.closed

    # The listener's own thread sets the OFF status after the listener returns.
    deadline = time.monotonic() + 5
    while client.data_source_status_provider.status.state != DataSourceState.OFF and time.monotonic() < deadline:
        time.sleep(0.01)
    assert client.data_source_status_provider.status.state == DataSourceState.OFF

    # The update that called the listener is not written to the closed store.
    assert store.writes_after_close == []


class _CloseTrackingStore(InMemoryFeatureStore):
    def __init__(self):
        super().__init__()
        self.closed = False
        self.fail_writes = False
        self.writes_after_close: List[str] = []

    def init(self, all_data):
        self._record_write('init')
        super().init(all_data)

    def upsert(self, kind, item):
        self._record_write('upsert')
        super().upsert(kind, item)

    def _record_write(self, operation: str):
        if self.closed:
            self.writes_after_close.append(operation)
        if self.fail_writes:
            raise Exception("simulated store failure")

    def is_monitoring_enabled(self) -> bool:
        return True

    def is_available(self) -> bool:
        return not self.fail_writes

    def close(self):
        self.closed = True
