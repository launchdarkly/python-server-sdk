"""
Tests for the shutdown contract of LDClient.close().

close() is documented as "Releases all threads and network connections used by the LaunchDarkly
client". These tests pin that contract. They are currently marked xfail because the SDK does not
yet honour it; each one describes a specific way the contract is broken.
"""

import time

import pytest

from ldclient.client import Config, LDClient
from ldclient.config import BigSegmentsConfig
from ldclient.interfaces import (
    BigSegmentStore,
    BigSegmentStoreMetadata,
    EventProcessor,
    UpdateProcessor
)

unreachable_uri = "http://fake"


class RecordingBigSegmentStore(BigSegmentStore):
    """A user-supplied big segment store, of the kind an application would provide."""

    def __init__(self, log: list):
        self._log = log

    def get_metadata(self) -> BigSegmentStoreMetadata:
        return BigSegmentStoreMetadata(int(time.time() * 1000))

    def get_membership(self, user_hash: str):
        return None

    def stop(self):
        self._log.append('big_segment_store')


class RecordingUpdateProcessor(UpdateProcessor):
    def __init__(self, log: list):
        self._log = log

    def start(self):
        pass

    def stop(self):
        self._log.append('update_processor')

    def initialized(self):
        return True


class FailingEventProcessor(EventProcessor):
    """
    Stands in for any component whose stop() raises. This is not far-fetched: close() reaches
    third-party code in two places - the eventsource client, and the application's own
    BigSegmentStore implementation.
    """

    def start(self):
        pass

    def stop(self):
        raise Exception("deliberate error from a component's stop()")

    def send_event(self, event):
        pass

    def flush(self):
        pass


def make_client(stop_log: list, event_processor_class=None) -> LDClient:
    config = Config(
        sdk_key='SDK_KEY',
        base_uri=unreachable_uri,
        events_uri=unreachable_uri,
        stream_uri=unreachable_uri,
        event_processor_class=event_processor_class or (lambda config: FailingEventProcessor()),
        update_processor_class=lambda config, store, ready: RecordingUpdateProcessor(stop_log),
        big_segments=BigSegmentsConfig(store=RecordingBigSegmentStore(stop_log)),
    )
    return LDClient(config=config, start_wait=0)


@pytest.mark.xfail(strict=True, reason="close() has no error handling, so a failure in one component orphans the rest")
def test_close_releases_every_component_even_if_one_raises():
    """
    INVARIANT: close() releases all of the client's resources. A component that fails to shut
    down cleanly must not prevent the remaining components from being released.

    close() calls the event processor, the data system and the big segment store manager in
    sequence with no error handling, so an exception from the first abandons the other two.
    Two of the three reach code the SDK does not control - the eventsource client and the
    application's own BigSegmentStore - so a raise here is a realistic scenario, not a contrived
    one. The result is leaked threads and connections from a client the caller believes is closed.
    """
    stop_log: list = []
    client = make_client(stop_log)

    try:
        client.close()
    except Exception:
        pass  # whether close() propagates is a separate question; the leak is the bug

    assert 'update_processor' in stop_log, "the data system was never stopped"
    assert 'big_segment_store' in stop_log, "the big segment store was never stopped"


@pytest.mark.xfail(strict=True, reason="close() has no closed-flag, so it re-runs shutdown on every call")
def test_close_is_idempotent():
    """
    INVARIANT: closing an already-closed client has no further effect.

    LDClient has no closed-flag, so a second close() runs the whole sequence again. That calls
    stop() a second time on the application's own BigSegmentStore, and on FDv2 calls
    store.close() twice. Double-close is easy to reach by accident - an explicit close() inside
    a `with` block does it, as does any cleanup path that runs more than once.
    """
    stop_log: list = []

    class NoopEventProcessor(EventProcessor):
        def start(self):
            pass

        def stop(self):
            pass

        def send_event(self, event):
            pass

        def flush(self):
            pass

    client = make_client(stop_log, event_processor_class=lambda config: NoopEventProcessor())

    client.close()
    client.close()

    assert stop_log.count('big_segment_store') == 1, "the big segment store was stopped more than once"
