from ldclient.client import LDClient, Config
from ldclient.interfaces import DataSourceState
from ldclient.config import BigSegmentsConfig
from testing.mock_components import MockBigSegmentStore
from testing.stub_util import MockEventProcessor, MockUpdateProcessor, make_put_event, stream_content
from testing.http_util import start_server

from queue import Queue


def test_big_segment_store_status_unavailable():
    config=Config(
        sdk_key='SDK_KEY',
        event_processor_class=MockEventProcessor,
        update_processor_class=MockUpdateProcessor
    )
    client = LDClient(config)
    assert client.big_segment_store_status_provider.status.available == False

def test_big_segment_store_status_updates():
    segstore = MockBigSegmentStore()
    segstore.setup_metadata_always_up_to_date()
    config=Config(
        sdk_key='SDK_KEY',
        big_segments=BigSegmentsConfig(store=segstore, status_poll_interval=0.01),
        event_processor_class=MockEventProcessor,
        update_processor_class=MockUpdateProcessor
    )
    statuses = Queue()

    with LDClient(config) as client:
        client.big_segment_store_status_provider.add_listener(lambda status: statuses.put(status))

        status1 = client.big_segment_store_status_provider.status
        assert status1.available == True
        assert status1.stale == False

        segstore.setup_metadata_always_stale()

        status2 = statuses.get(True, 1.0)
        assert status2.available == True
        assert status2.stale == True

        segstore.setup_metadata_always_up_to_date()

        status3 = statuses.get(True, 1.0)
        assert status3.available == True
        assert status3.stale == False
        assert client.big_segment_store_status_provider.status.available == True


def test_data_source_status_default():
    config=Config(
        sdk_key='SDK_KEY',
        event_processor_class=MockEventProcessor,
        update_processor_class=MockUpdateProcessor
    )
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
