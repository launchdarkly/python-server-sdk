# pylint: disable=missing-docstring

import asyncio
from typing import Any, AsyncGenerator, Dict, List, Mapping, Optional

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.config import (
    DataSourceBuilder,
    DataSourceBuilderConfig,
    DataSystemConfig
)
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.async_fdv2 import (
    AsyncFDv2,
    _AsyncFeatureStoreClientWrapper
)
from ldclient.impl.util import _LD_FD_FALLBACK_HEADER, _Fail, _Success
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    AsyncFeatureStore,
    Basis,
    BasisResult,
    ChangeSetBuilder,
    DataSourceState,
    DataSourceStatus,
    DataStoreStatus,
    FlagChange,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    Update
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class MockAsyncSynchronizer:
    """A controllable async synchronizer for testing."""

    def __init__(self, updates: Optional[List[Update]] = None):
        self._updates = updates or []
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stopped = False
        # Pre-populate the queue with provided updates
        for u in self._updates:
            self._queue.put_nowait(u)

    @property
    def name(self) -> str:
        return "MockAsyncSynchronizer"

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        while not self._stopped:
            try:
                update = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                yield update
            except asyncio.TimeoutError:
                continue

    async def stop(self) -> None:
        self._stopped = True

    async def push(self, update: Update):
        await self._queue.put(update)


class MockAsyncSynchronizerBuilder(DataSourceBuilder):
    def __init__(self, synchronizer: MockAsyncSynchronizer):
        self._sync = synchronizer

    def build(self, config: DataSourceBuilderConfig):
        return self._sync


class MockAsyncInitializer:
    """A controllable async initializer for testing."""

    def __init__(self, result: BasisResult):
        self._result = result

    @property
    def name(self) -> str:
        return "MockAsyncInitializer"

    async def fetch(self, ss: SelectorStore) -> BasisResult:
        return self._result


class MockAsyncInitializerBuilder(DataSourceBuilder):
    def __init__(self, initializer: MockAsyncInitializer):
        self._init = initializer

    def build(self, config: DataSourceBuilderConfig):
        return self._init


def _make_valid_basis() -> Basis:
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    builder.add_put(ObjectKind.FLAG, "my-flag", 1, {"key": "my-flag", "version": 1})
    selector = Selector(state="p:test:1", version=1)
    change_set = builder.finish(selector)
    return Basis(change_set=change_set, persist=False, environment_id=None)


def _make_valid_update() -> Update:
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    builder.add_put(ObjectKind.FLAG, "my-flag", 1, {"key": "my-flag", "version": 1})
    selector = Selector(state="p:test:1", version=1)
    change_set = builder.finish(selector)
    return Update(state=DataSourceState.VALID, change_set=change_set)


@pytest.mark.asyncio
async def test_async_fdv2_basic_start_stop():
    td = TestDataV2.data_source()
    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert ready_event.is_set()

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_synchronizer_receives_updates():
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    # Data should be available
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    # Check we can read the flag
    store = fdv2.store
    flag = await store.get(FEATURES, "feature-flag")
    assert flag is not None
    assert flag["key"] == "feature-flag"

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_flag_change_listener():
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)

    changes: List[FlagChange] = []
    flag_changed = asyncio.Event()

    def listener(change: FlagChange):
        changes.append(change)
        if len(changes) >= 2:
            flag_changed.set()

    fdv2.flag_change_listeners.add(listener)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    # Trigger another update
    td.update(td.flag("feature-flag").on(False))

    await asyncio.wait_for(flag_changed.wait(), timeout=2)
    assert len(changes) >= 2
    assert all(c.key == "feature-flag" for c in changes)

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_two_phase_init():
    td_initializer = TestDataV2.data_source()
    td_initializer.update(td_initializer.flag("feature-flag").on(True))

    td_synchronizer = TestDataV2.data_source()
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(True))
    td_synchronizer.update(td_synchronizer.flag("feature-flag").on(False))

    data_system_config = DataSystemConfig(
        initializers=[td_initializer.async_builder],
        synchronizers=[td_synchronizer.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_initializer_async():
    """Test with a pure async initializer."""
    basis = _make_valid_basis()
    init = MockAsyncInitializer(_Success(basis))
    init_builder = MockAsyncInitializerBuilder(init)

    # Empty synchronizer that just keeps running
    sync_mock = MockAsyncSynchronizer()
    sync_builder = MockAsyncSynchronizerBuilder(sync_mock)

    data_system_config = DataSystemConfig(
        initializers=[init_builder],
        synchronizers=[sync_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_fallsback_to_secondary_synchronizer():
    """When primary synchronizer yields nothing, should move to secondary."""
    td = TestDataV2.data_source()
    td.update(td.flag("feature-flag").on(True))

    # An async synchronizer that immediately stops (produces no updates)
    empty_sync = MockAsyncSynchronizer()
    empty_sync._stopped = True  # pre-stopped — yields nothing
    empty_builder = MockAsyncSynchronizerBuilder(empty_sync)

    data_system_config = DataSystemConfig(
        initializers=[td.async_builder],
        synchronizers=[empty_builder, td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_falls_back_to_fdv1_on_synchronizer_signal():
    """Synchronizer yielding fallback_to_fdv1=True triggers FDv1 fallback."""
    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    # Primary synchronizer signals FDv1 fallback
    fallback_update = Update(state=DataSourceState.OFF, fallback_to_fdv1=True)
    primary_sync = MockAsyncSynchronizer([fallback_update])
    primary_builder = MockAsyncSynchronizerBuilder(primary_sync)

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[primary_builder],
        fdv1_fallback_synchronizer=td_fdv1.async_builder,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)

    store = fdv2.store
    flag = await store.get(FEATURES, "fdv1-flag")
    assert flag is not None

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_data_availability_defaults_when_no_sources():
    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=None,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    # No sources means target is CACHED, and data is also CACHED (or DEFAULTS)
    assert fdv2.target_availability == DataAvailability.CACHED

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_data_availability_refreshed_with_data():
    td = TestDataV2.data_source()
    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    assert (await fdv2.data_availability()).at_least(DataAvailability.REFRESHED)
    assert fdv2.target_availability.at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_disabled_immediately_signals_ready():
    td = TestDataV2.data_source()
    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[td.async_builder],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy", offline=True), data_system_config)
    fdv2.start(ready_event)

    # Should be ready immediately because disabled
    await asyncio.wait_for(ready_event.wait(), timeout=1)
    assert ready_event.is_set()

    await fdv2.stop()


class ListAsyncSynchronizer:
    """An async synchronizer that yields a fixed list of updates and then
    completes. A completed iterator signals a permanent failure to the
    coordinator, so the coordinator moves on to the next synchronizer."""

    def __init__(self, name: str, updates: List[Update]):
        self._name = name
        self._updates = updates
        self.sync_called = False

    @property
    def name(self) -> str:
        return self._name

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        self.sync_called = True
        for update in self._updates:
            yield update

    async def stop(self) -> None:
        pass


class RecordingAsyncInitializer:
    """An async initializer that returns a fixed result and counts calls."""

    def __init__(self, name: str, result: BasisResult):
        self._name = name
        self._result = result
        self.call_count = 0

    @property
    def name(self) -> str:
        return self._name

    async def fetch(self, ss: SelectorStore) -> BasisResult:
        self.call_count += 1
        return self._result


async def _status_reaches(fdv2: AsyncFDv2, state: DataSourceState, timeout: float = 2.0):
    deadline = 0
    while deadline < int(timeout / 0.01):
        if fdv2.data_source_status_provider.status.state == state:
            return
        await asyncio.sleep(0.01)
        deadline += 1


@pytest.mark.asyncio
async def test_async_fdv2_both_synchronizers_fail_transitions_to_off():
    """Both synchronizers complete without data -> data source goes OFF."""
    primary = ListAsyncSynchronizer("primary", [])
    secondary = ListAsyncSynchronizer("secondary", [])

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[
            MockAsyncSynchronizerBuilder(primary),  # type: ignore[arg-type]
            MockAsyncSynchronizerBuilder(secondary),  # type: ignore[arg-type]
        ],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    await _status_reaches(fdv2, DataSourceState.OFF)

    assert fdv2.data_source_status_provider.status.state == DataSourceState.OFF
    assert primary.sync_called
    assert secondary.sync_called

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_initializer_header_fallback_engages_fdv1():
    """An initializer error carrying X-LD-FD-Fallback engages the FDv1
    fallback synchronizer, and the configured FDv2 synchronizer must not run."""
    init = RecordingAsyncInitializer(
        "hdr-fallback",
        _Fail(error="boom", exception=None, headers={_LD_FD_FALLBACK_HEADER: 'true'}),
    )

    # This FDv2 synchronizer must never run because we fell back during init.
    fdv2_sync = ListAsyncSynchronizer("fdv2-should-not-run", [])

    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-flag").on(True))

    data_system_config = DataSystemConfig(
        initializers=[MockAsyncInitializerBuilder(init)],  # type: ignore[arg-type]
        synchronizers=[MockAsyncSynchronizerBuilder(fdv2_sync)],  # type: ignore[arg-type]
        fdv1_fallback_synchronizer=td_fdv1.async_builder,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    flag = await fdv2.store.get(FEATURES, "fdv1-flag")
    assert flag is not None
    assert fdv2_sync.sync_called is False

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_initializer_header_fallback_without_fdv1_transitions_to_off():
    """An initializer signals FDv1 fallback but no FDv1 synchronizer is
    configured -> the data source transitions to OFF."""
    init = RecordingAsyncInitializer(
        "hdr-fallback-no-fdv1",
        _Fail(error="boom", exception=None, headers={_LD_FD_FALLBACK_HEADER: 'true'}),
    )

    fdv2_sync = ListAsyncSynchronizer("fdv2-should-not-run", [])

    data_system_config = DataSystemConfig(
        initializers=[MockAsyncInitializerBuilder(init)],  # type: ignore[arg-type]
        synchronizers=[MockAsyncSynchronizerBuilder(fdv2_sync)],  # type: ignore[arg-type]
        fdv1_fallback_synchronizer=None,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)
    await _status_reaches(fdv2, DataSourceState.OFF)

    assert fdv2.data_source_status_provider.status.state == DataSourceState.OFF
    assert fdv2_sync.sync_called is False

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_interrupted_without_header_falls_back_to_secondary():
    """An INTERRUPTED update without the fallback header moves to the next
    synchronizer, not to FDv1."""
    primary = ListAsyncSynchronizer(
        "primary",
        [Update(state=DataSourceState.INTERRUPTED, fallback_to_fdv1=False)],
    )
    secondary = ListAsyncSynchronizer(
        "secondary",
        [Update(state=DataSourceState.VALID, fallback_to_fdv1=False)],
    )

    td_fdv1 = TestDataV2.data_source()
    td_fdv1.update(td_fdv1.flag("fdv1-should-not-appear").on(True))

    data_system_config = DataSystemConfig(
        initializers=None,
        synchronizers=[
            MockAsyncSynchronizerBuilder(primary),  # type: ignore[arg-type]
            MockAsyncSynchronizerBuilder(secondary),  # type: ignore[arg-type]
        ],
        fdv1_fallback_synchronizer=td_fdv1.async_builder,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    assert primary.sync_called
    assert secondary.sync_called
    # FDv1 must not have been engaged.
    assert await fdv2.store.get(FEATURES, "fdv1-should-not-appear") is None

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_initializers_run_until_success():
    """Initializers run in order until one succeeds; a failing initializer is
    skipped and the next one is tried."""
    fail_init = RecordingAsyncInitializer("fail", _Fail(error="boom", exception=None))
    success_init = RecordingAsyncInitializer("ok", _Success(_make_valid_basis()))

    # An empty synchronizer that keeps running after initialization.
    sync_mock = MockAsyncSynchronizer()

    data_system_config = DataSystemConfig(
        initializers=[
            MockAsyncInitializerBuilder(fail_init),  # type: ignore[arg-type]
            MockAsyncInitializerBuilder(success_init),  # type: ignore[arg-type]
        ],
        synchronizers=[MockAsyncSynchronizerBuilder(sync_mock)],
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    assert fail_init.call_count == 1
    assert success_init.call_count == 1
    assert await fdv2.store.get(FEATURES, "my-flag") is not None

    await fdv2.stop()


@pytest.mark.asyncio
async def test_async_fdv2_initializers_stop_on_first_success():
    """Once an initializer returns a basis with a defined selector, the
    remaining initializers are skipped."""
    first = RecordingAsyncInitializer("first", _Success(_make_valid_basis()))
    second = RecordingAsyncInitializer("second", _Success(_make_valid_basis()))

    data_system_config = DataSystemConfig(
        initializers=[
            MockAsyncInitializerBuilder(first),  # type: ignore[arg-type]
            MockAsyncInitializerBuilder(second),  # type: ignore[arg-type]
        ],
        synchronizers=None,
    )

    ready_event = asyncio.Event()
    fdv2 = AsyncFDv2(AsyncConfig(sdk_key="dummy"), data_system_config)
    fdv2.start(ready_event)

    await asyncio.wait_for(ready_event.wait(), timeout=2)

    assert first.call_count == 1
    assert second.call_count == 0

    await fdv2.stop()


class FakeAsyncStore(AsyncFeatureStore):
    """An async store whose operations can be made to fail on demand."""

    def __init__(self):
        self._data: Dict[VersionedDataKind, Dict[str, dict]] = {FEATURES: {}, SEGMENTS: {}}
        self._inited = False
        self._available = True
        self.fail = False
        self.init_calls: List[Mapping] = []
        self.closed = False

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        if self.fail:
            raise RuntimeError("store down")
        self.init_calls.append(all_data)
        self._data = {FEATURES: dict(all_data.get(FEATURES, {})), SEGMENTS: dict(all_data.get(SEGMENTS, {}))}
        self._inited = True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        if self.fail:
            raise RuntimeError("store down")
        return self._data.get(kind, {}).get(key)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        if self.fail:
            raise RuntimeError("store down")
        return dict(self._data.get(kind, {}))

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        if self.fail:
            raise RuntimeError("store down")
        self._data[kind][item["key"]] = item
        return True

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        return await self.upsert(kind, {"key": key, "version": version, "deleted": True})

    @property
    def initialized(self) -> bool:
        return self._inited

    async def is_initialized(self) -> bool:
        if self.fail:
            raise RuntimeError("store down")
        return self._inited

    async def is_available(self) -> bool:
        return self._available

    def is_monitoring_enabled(self) -> bool:
        return True

    async def close(self) -> None:
        self.closed = True


class StoreWithoutAvailability(AsyncFeatureStore):
    async def init(self, all_data):
        pass

    async def get(self, kind, key):
        return None

    async def all(self, kind):
        return {}

    async def upsert(self, kind, item):
        return True

    async def delete(self, kind, key, version):
        return True

    @property
    def initialized(self) -> bool:
        return True

    async def is_initialized(self) -> bool:
        return True


class StoreWithAvailabilityNoOptIn(StoreWithoutAvailability):
    """Reports availability but does not opt in to monitoring.

    Models a custom store that provides is_available yet omits
    is_monitoring_enabled, which must not turn on availability polling.
    """

    async def is_available(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_is_monitoring_enabled_true_when_store_opts_in():
    wrapper = _AsyncFeatureStoreClientWrapper(FakeAsyncStore(), lambda _s: None)
    assert wrapper.is_monitoring_enabled() is True


@pytest.mark.asyncio
async def test_is_monitoring_enabled_false_without_is_available():
    wrapper = _AsyncFeatureStoreClientWrapper(StoreWithoutAvailability(), lambda _s: None)
    assert wrapper.is_monitoring_enabled() is False


@pytest.mark.asyncio
async def test_is_monitoring_enabled_false_when_store_does_not_opt_in():
    # A store with is_available but no is_monitoring_enabled must not be polled.
    wrapper = _AsyncFeatureStoreClientWrapper(StoreWithAvailabilityNoOptIn(), lambda _s: None)
    assert wrapper.is_monitoring_enabled() is False


@pytest.mark.asyncio
async def test_init_sorts_and_delegates():
    store = FakeAsyncStore()
    wrapper = _AsyncFeatureStoreClientWrapper(store, lambda _s: None)
    await wrapper.init({FEATURES: {}, SEGMENTS: {}})
    assert len(store.init_calls) == 1
    assert wrapper.initialized is True


@pytest.mark.asyncio
async def test_failure_marks_unavailable_polls_and_recovers():
    store = FakeAsyncStore()
    statuses: List[DataStoreStatus] = []
    wrapper = _AsyncFeatureStoreClientWrapper(store, lambda s: statuses.append(s))

    # Make the next operation fail.
    store.fail = True
    store._available = False

    with pytest.raises(RuntimeError):
        await wrapper.get(FEATURES, "flag-a")

    # The wrapper reported unavailability and started a poller.
    assert len(statuses) == 1
    assert statuses[0].available is False

    # Bring the store back; the poller (0.5s interval) should notice and recover.
    store.fail = False
    store._available = True

    for _ in range(40):
        await asyncio.sleep(0.05)
        if len(statuses) >= 2:
            break

    assert len(statuses) == 2
    assert statuses[1].available is True

    await wrapper.close()


@pytest.mark.asyncio
async def test_close_stops_poller_and_closes_inner():
    store = FakeAsyncStore()
    statuses: List[DataStoreStatus] = []
    wrapper = _AsyncFeatureStoreClientWrapper(store, lambda s: statuses.append(s))

    # Trigger an outage so a poller is running.
    store.fail = True
    store._available = False
    with pytest.raises(RuntimeError):
        await wrapper.all(FEATURES)

    assert wrapper._poller is not None

    await wrapper.close()

    assert wrapper._poller is None
    assert store.closed is True


@pytest.mark.asyncio
async def test_successful_ops_pass_through():
    store = FakeAsyncStore()
    wrapper = _AsyncFeatureStoreClientWrapper(store, lambda _s: None)

    await wrapper.upsert(FEATURES, {"key": "flag-a", "version": 1})
    got = await wrapper.get(FEATURES, "flag-a")
    assert got is not None and got["key"] == "flag-a"
    allf = await wrapper.all(FEATURES)
    assert "flag-a" in allf
