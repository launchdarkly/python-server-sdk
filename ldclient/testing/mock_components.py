import threading
import time
from typing import Any, Callable, Dict, Optional

from ldclient.impl.util import _Success
from ldclient.interfaces import (
    Basis,
    BigSegmentStore,
    BigSegmentStoreMetadata,
    ChangeSetBuilder,
    IntentCode,
    ObjectKind,
    OverrideSink,
    Selector
)


class MockBigSegmentStore(BigSegmentStore):
    def __init__(self):
        self.__get_metadata = lambda: BigSegmentStoreMetadata(time.time())
        self.__memberships = {}
        self.__membership_queries = []
        self.setup_metadata_always_up_to_date()

    def get_metadata(self) -> BigSegmentStoreMetadata:
        return self.__get_metadata()

    def get_membership(self, user_hash: str) -> dict:
        self.__membership_queries.append(user_hash)
        return self.__memberships.get(user_hash, None)

    def setup_metadata(self, callback: Callable[[], BigSegmentStoreMetadata]):
        self.__get_metadata = callback

    def setup_metadata_always_up_to_date(self):
        self.setup_metadata(lambda: BigSegmentStoreMetadata(time.time() * 1000))

    def setup_metadata_always_stale(self):
        self.setup_metadata(lambda: BigSegmentStoreMetadata(0))

    def setup_metadata_none(self):
        self.setup_metadata(lambda: None)

    def setup_metadata_error(self):
        self.setup_metadata(self.__fail)

    def setup_membership(self, user_hash: str, membership: dict):
        self.__memberships[user_hash] = membership

    @property
    def membership_queries(self) -> list:
        return self.__membership_queries.copy()

    def __fail(self):
        raise Exception("deliberate error")


class MockSelectorStore():
    def __init__(self, selector: Selector):
        self._selector = selector

    def selector(self) -> Selector:
        return self._selector


class MockOverrideSource:
    """
    An override source for tests. It pushes its current contents to the sink when started and
    on every later call to set_overrides, and records its lifecycle calls.
    """

    def __init__(self, flags: Optional[Dict[str, Any]] = None, segments: Optional[Dict[str, Any]] = None):
        self._flags: Dict[str, Any] = dict(flags or {})
        self._segments: Dict[str, Any] = dict(segments or {})
        self._sink: Optional[OverrideSink] = None
        self.start_count = 0
        self.close_count = 0

    def start(self, sink: OverrideSink) -> None:
        self.start_count += 1
        self._sink = sink
        sink.set_overrides(self._flags, self._segments)

    def close(self) -> None:
        self.close_count += 1

    def set_overrides(self, flags: Optional[Dict[str, Any]] = None, segments: Optional[Dict[str, Any]] = None) -> None:
        """Replaces the source's contents and, once started, pushes them to the sink."""
        self._flags = dict(flags or {})
        self._segments = dict(segments or {})
        if self._sink is not None:
            self._sink.set_overrides(self._flags, self._segments)

    @property
    def builder(self) -> 'MockOverrideSourceBuilder':
        return MockOverrideSourceBuilder(self)


class MockOverrideSourceBuilder:
    """Wraps a MockOverrideSource so it can be passed to the data system configuration."""

    def __init__(self, source: MockOverrideSource):
        self._source = source
        self.build_count = 0

    def build(self, config) -> MockOverrideSource:
        self.build_count += 1
        return self._source


class FailingOverrideSourceBuilder:
    """A builder whose build raises, to simulate invalid override source configuration."""

    def build(self, config):
        raise ValueError("invalid override source configuration")


class HangingSynchronizer:
    """
    A synchronizer that connects but never yields data, so the client stays uninitialized
    until it is stopped.
    """

    def __init__(self):
        self._stop = threading.Event()

    @property
    def name(self) -> str:
        return "HangingSynchronizer"

    def sync(self, ss):
        self._stop.wait()
        yield from ()

    def stop(self):
        self._stop.set()

    @property
    def builder(self) -> 'MockDataSourceBuilder':
        return MockDataSourceBuilder(self)


class StaticInitializer:
    """
    An initializer that supplies fixed flag and segment definitions as a full-transfer basis
    with a defined selector, so the client reports full data availability once it has applied
    the data.
    """

    def __init__(self, flags: Optional[Dict[str, dict]] = None, segments: Optional[Dict[str, dict]] = None):
        self._flags = dict(flags or {})
        self._segments = dict(segments or {})

    @property
    def name(self) -> str:
        return "StaticInitializer"

    def fetch(self, ss):
        builder = ChangeSetBuilder()
        builder.start(IntentCode.TRANSFER_FULL)
        for key, flag in self._flags.items():
            builder.add_put(ObjectKind.FLAG, key, flag.get('version', 1), flag)
        for key, segment in self._segments.items():
            builder.add_put(ObjectKind.SEGMENT, key, segment.get('version', 1), segment)
        change_set = builder.finish(Selector.new_selector('test-state', 1))
        return _Success(Basis(change_set=change_set, persist=False, environment_id=None))

    @property
    def builder(self) -> 'MockDataSourceBuilder':
        return MockDataSourceBuilder(self)


class MockDataSourceBuilder:
    """Wraps a ready-made data source so it can be passed to the data system configuration."""

    def __init__(self, source):
        self._source = source

    def build(self, config):
        return self._source
