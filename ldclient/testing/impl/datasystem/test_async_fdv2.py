# pylint: disable=missing-docstring

import asyncio
from typing import AsyncGenerator, List, Optional

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.config import (
    DataSourceBuilder,
    DataSourceBuilderConfig,
    DataSystemConfig
)
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.async_fdv2 import AsyncFDv2
from ldclient.impl.util import _Fail, _Success
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSetBuilder,
    DataSourceState,
    DataSourceStatus,
    FlagChange,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    Update
)
from ldclient.versioned_data_kind import FEATURES

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Basic start/stop cycle
# ---------------------------------------------------------------------------


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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

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


# ---------------------------------------------------------------------------
# Initializer + synchronizer
# ---------------------------------------------------------------------------


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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


# ---------------------------------------------------------------------------
# Fallback / failover
# ---------------------------------------------------------------------------


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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)

    store = fdv2.store
    flag = await store.get(FEATURES, "fdv1-flag")
    assert flag is not None

    await fdv2.stop()


# ---------------------------------------------------------------------------
# Data availability
# ---------------------------------------------------------------------------


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
    assert fdv2.data_availability.at_least(DataAvailability.REFRESHED)
    assert fdv2.target_availability.at_least(DataAvailability.REFRESHED)

    await fdv2.stop()


# ---------------------------------------------------------------------------
# Disabled (offline) mode
# ---------------------------------------------------------------------------


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
