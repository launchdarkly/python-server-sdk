import asyncio
from datetime import datetime, timedelta
from typing import List

import pytest

from ldclient import Result
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.model import FeatureFlag
from ldclient.impl.util import timedelta_millis
from ldclient.migrations import (
    AsyncMigrator,
    AsyncMigratorBuilder,
    AsyncMigratorFn
)
from ldclient.migrations.tracker import MigrationOpEvent, OpTracker
from ldclient.migrations.types import ExecutionOrder, Origin, Stage
from ldclient.testing.builders import FlagBuilder

user = Context.from_dict({u'key': u'xyz', u'kind': u'user', u'bizzle': u'def'})


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeEventProcessor:
    """Records events the way the real (async/sync) event processors expose them
    so tests can assert on the emitted MigrationOpEvent, mirroring the sync
    migrator tests' use of ``client._event_processor._events``."""

    def __init__(self):
        self._events: List = []

    def send_event(self, event):
        self._events.append(event)


class FakeAsyncClient:
    """A minimal stand-in for AsyncLDClient that exposes the exact surface the
    AsyncMigrator depends on: an async ``migration_variation`` returning
    ``(Stage, OpTracker)`` and a synchronous ``track_migration_op``.

    The flag key passed to ``migration_variation`` is interpreted as a Stage
    value (matching the sync migrator tests), and a real OpTracker is built from
    a real FeatureFlag so the consistency/error/latency/event-build semantics
    are genuinely exercised.
    """

    def __init__(self):
        self._event_processor = FakeEventProcessor()
        self._flags = {}
        for stage in Stage:
            flag = FlagBuilder(stage.value).on(True).variations(stage.value).fallthrough_variation(0).build()
            self._flags[stage.value] = flag

    async def migration_variation(self, key: str, context: Context, default_stage: Stage):
        # Yield once to confirm callers truly await this coroutine.
        await asyncio.sleep(0)
        flag: FeatureFlag = self._flags[key]
        stage = Stage(key)
        detail = EvaluationDetail(stage.value, 0, {'kind': 'FALLTHROUGH'})
        tracker = OpTracker(key, flag, context, detail, default_stage)
        return stage, tracker

    def track_migration_op(self, tracker: OpTracker):
        # Synchronous on the real async client; must NOT be awaited.
        event = tracker.build()
        if isinstance(event, str):
            raise AssertionError("tracker.build() failed: %s" % event)
        # Emulate the EventInputEvaluation that migration_variation would have
        # queued, so index [1] is the MigrationOpEvent (as in the sync tests).
        self._event_processor.send_event(_FakeEvalEvent())
        self._event_processor.send_event(event)


class _FakeEvalEvent(EventInputEvaluation):
    def __init__(self):
        pass


async def async_success(payload) -> Result:
    return Result.success(True)


def raises_exception(msg) -> AsyncMigratorFn:
    async def inner(payload):
        raise Exception(msg)

    return inner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def builder() -> AsyncMigratorBuilder:
    client = FakeAsyncClient()
    builder = AsyncMigratorBuilder(client)  # type: ignore[arg-type]
    builder.track_latency(False)
    builder.track_errors(False)

    builder.read(async_success, async_success, None)
    builder.write(async_success, async_success)

    return builder


# ---------------------------------------------------------------------------
# Builder validation
# ---------------------------------------------------------------------------

class TestBuilder:
    def test_can_build_successfully(self):
        client = FakeAsyncClient()
        builder = AsyncMigratorBuilder(client)  # type: ignore[arg-type]
        builder.read(async_success, async_success, None)
        builder.write(async_success, async_success)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

    @pytest.mark.parametrize(
        "order",
        [
            pytest.param(ExecutionOrder.SERIAL, id="serial"),
            pytest.param(ExecutionOrder.RANDOM, id="random"),
            pytest.param(ExecutionOrder.PARALLEL, id="parallel"),
        ],
    )
    def test_can_modify_execution_order(self, order):
        client = FakeAsyncClient()
        builder = AsyncMigratorBuilder(client)  # type: ignore[arg-type]
        builder.read(async_success, async_success, None)
        builder.write(async_success, async_success)
        builder.read_execution_order(order)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

    def test_build_fails_without_read(self):
        client = FakeAsyncClient()
        builder = AsyncMigratorBuilder(client)  # type: ignore[arg-type]
        builder.write(async_success, async_success)
        migrator = builder.build()
        assert isinstance(migrator, str)
        assert migrator == "read configuration not provided"

    def test_build_fails_without_write(self):
        client = FakeAsyncClient()
        builder = AsyncMigratorBuilder(client)  # type: ignore[arg-type]
        builder.read(async_success, async_success)
        migrator = builder.build()
        assert isinstance(migrator, str)
        assert migrator == "write configuration not provided"


# ---------------------------------------------------------------------------
# Payload passthrough
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestPassingPayloadThrough:
    @pytest.mark.parametrize(
        "stage,count",
        [
            pytest.param(Stage.OFF, 1, id="off"),
            pytest.param(Stage.DUALWRITE, 1, id="dualwrite"),
            pytest.param(Stage.SHADOW, 2, id="shadow"),
            pytest.param(Stage.LIVE, 2, id="live"),
            pytest.param(Stage.RAMPDOWN, 1, id="rampdown"),
            pytest.param(Stage.COMPLETE, 1, id="complete"),
        ],
    )
    async def test_passes_through_read(self, builder: AsyncMigratorBuilder, stage: Stage, count: int):
        payloads = []

        async def capture_payloads(payload):
            payloads.append(payload)
            return Result.success(None)

        builder.read(capture_payloads, capture_payloads)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE, "payload")

        assert result.is_success()
        assert len(payloads) == count
        assert all("payload" == p for p in payloads)

    @pytest.mark.parametrize(
        "stage,count",
        [
            pytest.param(Stage.OFF, 1, id="off"),
            pytest.param(Stage.DUALWRITE, 2, id="dualwrite"),
            pytest.param(Stage.SHADOW, 2, id="shadow"),
            pytest.param(Stage.LIVE, 2, id="live"),
            pytest.param(Stage.RAMPDOWN, 2, id="rampdown"),
            pytest.param(Stage.COMPLETE, 1, id="complete"),
        ],
    )
    async def test_passes_through_write(self, builder: AsyncMigratorBuilder, stage: Stage, count: int):
        payloads = []

        async def capture_payloads(payload):
            payloads.append(payload)
            return Result.success(None)

        builder.write(capture_payloads, capture_payloads)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE, "payload")

        assert result.authoritative.is_success()
        if result.nonauthoritative is not None:
            assert result.nonauthoritative.is_success()

        assert len(payloads) == count
        assert all("payload" == p for p in payloads)


# ---------------------------------------------------------------------------
# Invoked tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestTrackingInvoked:
    @pytest.mark.parametrize(
        "stage,origins",
        [
            pytest.param(Stage.OFF, [Origin.OLD], id="off"),
            pytest.param(Stage.DUALWRITE, [Origin.OLD], id="dualwrite"),
            pytest.param(Stage.SHADOW, [Origin.OLD, Origin.NEW], id="shadow"),
            pytest.param(Stage.LIVE, [Origin.OLD, Origin.NEW], id="live"),
            pytest.param(Stage.RAMPDOWN, [Origin.NEW], id="rampdown"),
            pytest.param(Stage.COMPLETE, [Origin.NEW], id="complete"),
        ],
    )
    async def test_reads(self, builder: AsyncMigratorBuilder, stage: Stage, origins: List[Origin]):
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.invoked)
        assert all(o in event.invoked for o in origins)

    @pytest.mark.parametrize(
        "stage,origins",
        [
            pytest.param(Stage.OFF, [Origin.OLD], id="off"),
            pytest.param(Stage.DUALWRITE, [Origin.OLD, Origin.NEW], id="dualwrite"),
            pytest.param(Stage.SHADOW, [Origin.OLD, Origin.NEW], id="shadow"),
            pytest.param(Stage.LIVE, [Origin.OLD, Origin.NEW], id="live"),
            pytest.param(Stage.RAMPDOWN, [Origin.OLD, Origin.NEW], id="rampdown"),
            pytest.param(Stage.COMPLETE, [Origin.NEW], id="complete"),
        ],
    )
    async def test_writes(self, builder: AsyncMigratorBuilder, stage: Stage, origins: List[Origin]):
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.invoked)
        assert all(o in event.invoked for o in origins)


# ---------------------------------------------------------------------------
# Latency tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestTrackingLatency:
    @pytest.mark.parametrize(
        "stage,origins",
        [
            pytest.param(Stage.OFF, [Origin.OLD], id="off"),
            pytest.param(Stage.DUALWRITE, [Origin.OLD], id="dualwrite"),
            pytest.param(Stage.SHADOW, [Origin.OLD, Origin.NEW], id="shadow"),
            pytest.param(Stage.LIVE, [Origin.OLD, Origin.NEW], id="live"),
            pytest.param(Stage.RAMPDOWN, [Origin.NEW], id="rampdown"),
            pytest.param(Stage.COMPLETE, [Origin.NEW], id="complete"),
        ],
    )
    async def test_reads(self, builder: AsyncMigratorBuilder, stage: Stage, origins: List[Origin]):
        async def delay(payload):
            await asyncio.sleep(0.1)
            return Result.success("success")

        builder.track_latency(True)
        builder.read(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.latencies)
        for o in origins:
            assert o in event.latencies
            assert event.latencies[o] >= timedelta(milliseconds=100)

    @pytest.mark.parametrize(
        "stage,origins",
        [
            pytest.param(Stage.OFF, [Origin.OLD], id="off"),
            pytest.param(Stage.DUALWRITE, [Origin.OLD, Origin.NEW], id="dualwrite"),
            pytest.param(Stage.SHADOW, [Origin.OLD, Origin.NEW], id="shadow"),
            pytest.param(Stage.LIVE, [Origin.OLD, Origin.NEW], id="live"),
            pytest.param(Stage.RAMPDOWN, [Origin.OLD, Origin.NEW], id="rampdown"),
            pytest.param(Stage.COMPLETE, [Origin.NEW], id="complete"),
        ],
    )
    async def test_writes(self, builder: AsyncMigratorBuilder, stage: Stage, origins: List[Origin]):
        async def delay(payload):
            await asyncio.sleep(0.1)
            return Result.success("success")

        builder.track_latency(True)
        builder.write(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.latencies)
        for o in origins:
            assert o in event.latencies
            assert event.latencies[o] >= timedelta(milliseconds=100)


# ---------------------------------------------------------------------------
# Error tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestTrackingErrors:
    @pytest.mark.parametrize(
        "stage,origins",
        [
            pytest.param(Stage.OFF, [Origin.OLD], id="off"),
            pytest.param(Stage.DUALWRITE, [Origin.OLD], id="dualwrite"),
            pytest.param(Stage.SHADOW, [Origin.OLD, Origin.NEW], id="shadow"),
            pytest.param(Stage.LIVE, [Origin.OLD, Origin.NEW], id="live"),
            pytest.param(Stage.RAMPDOWN, [Origin.NEW], id="rampdown"),
            pytest.param(Stage.COMPLETE, [Origin.NEW], id="complete"),
        ],
    )
    async def test_reads(self, builder: AsyncMigratorBuilder, stage: Stage, origins: List[Origin]):
        async def fail(_):
            return Result.fail("fail")

        builder.track_errors(True)
        builder.read(fail, fail)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)

        assert not result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.errors)
        assert all(o in event.errors for o in origins)

    @pytest.mark.parametrize(
        "stage,origin",
        [
            pytest.param(Stage.OFF, Origin.OLD, id="off"),
            pytest.param(Stage.DUALWRITE, Origin.OLD, id="dualwrite"),
            pytest.param(Stage.SHADOW, Origin.OLD, id="shadow"),
            pytest.param(Stage.LIVE, Origin.NEW, id="live"),
            pytest.param(Stage.RAMPDOWN, Origin.NEW, id="rampdown"),
            pytest.param(Stage.COMPLETE, Origin.NEW, id="complete"),
        ],
    )
    async def test_authoritative_writes(self, builder: AsyncMigratorBuilder, stage: Stage, origin: Origin):
        async def fail(_):
            return Result.fail("fail")

        builder.track_errors(True)
        builder.write(fail, fail)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert not result.authoritative.is_success()
        assert result.nonauthoritative is None
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert 1 == len(event.errors)
        assert origin in event.errors

    @pytest.mark.parametrize(
        "stage,fail_old,fail_new,origin",
        [
            pytest.param(Stage.DUALWRITE, False, True, Origin.NEW, id="dualwrite"),
            pytest.param(Stage.SHADOW, False, True, Origin.NEW, id="shadow"),
            pytest.param(Stage.LIVE, True, False, Origin.OLD, id="live"),
            pytest.param(Stage.RAMPDOWN, True, False, Origin.OLD, id="rampdown"),
        ],
    )
    async def test_nonauthoritative_writes(self, builder: AsyncMigratorBuilder, stage: Stage, fail_old: bool, fail_new: bool, origin: Origin):
        async def success(_):
            return Result.success(None)

        async def fail(_):
            return Result.fail("fail")

        builder.track_errors(True)
        builder.write(fail if fail_old else success, fail if fail_new else success)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        assert result.nonauthoritative is not None
        assert not result.nonauthoritative.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert 1 == len(event.errors)
        assert origin in event.errors


# ---------------------------------------------------------------------------
# Consistency tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestTrackingConsistency:
    @pytest.mark.parametrize(
        "stage",
        [
            pytest.param(Stage.OFF, id="off"),
            pytest.param(Stage.DUALWRITE, id="dualwrite"),
            pytest.param(Stage.RAMPDOWN, id="rampdown"),
            pytest.param(Stage.COMPLETE, id="complete"),
        ],
    )
    async def test_consistency_is_not_run_in_most_stages(self, builder: AsyncMigratorBuilder, stage: Stage):
        async def value(_):
            return Result.success("value")

        builder.read(value, value, lambda lhs, rhs: lhs == rhs)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is None

    @pytest.mark.parametrize(
        "stage,old,new,expected",
        [
            pytest.param(Stage.SHADOW, "value", "value", True, id="shadow matches"),
            pytest.param(Stage.LIVE, "value", "value", True, id="live matches"),
            pytest.param(Stage.SHADOW, "old", "new", False, id="shadow does not match"),
            pytest.param(Stage.LIVE, "old", "new", False, id="live does not match"),
        ],
    )
    async def test_consistency_is_tracked_correctly(self, builder: AsyncMigratorBuilder, stage: Stage, old: str, new: str, expected: bool):
        async def old_fn(_):
            return Result.success(old)

        async def new_fn(_):
            return Result.success(new)

        builder.read(old_fn, new_fn, lambda lhs, rhs: lhs == rhs)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is expected

    @pytest.mark.parametrize(
        "stage,old,new",
        [
            pytest.param(Stage.SHADOW, "value", "value", id="shadow"),
            pytest.param(Stage.LIVE, "value", "value", id="live"),
        ],
    )
    async def test_consistency_handles_exceptions(self, builder: AsyncMigratorBuilder, stage: Stage, old: str, new: str):
        def raise_exception(lhs, rhs):
            raise Exception("error")

        async def old_fn(_):
            return Result.success(old)

        async def new_fn(_):
            return Result.success(new)

        builder.read(old_fn, new_fn, raise_exception)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        event = builder._client._event_processor._events[1]  # type: ignore
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is None


# ---------------------------------------------------------------------------
# Exceptions in migrator functions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestHandlesExceptionsInMigratorFn:
    @pytest.mark.parametrize(
        "stage,expected_msg",
        [
            pytest.param(Stage.OFF, "old read", id="off"),
            pytest.param(Stage.DUALWRITE, "old read", id="dualwrite"),
            pytest.param(Stage.SHADOW, "old read", id="shadow"),
            pytest.param(Stage.LIVE, "new read", id="live"),
            pytest.param(Stage.RAMPDOWN, "new read", id="rampdown"),
            pytest.param(Stage.COMPLETE, "new read", id="complete"),
        ],
    )
    async def test_reads(self, builder: AsyncMigratorBuilder, stage: Stage, expected_msg: str):
        builder.read(raises_exception("old read"), raises_exception("new read"))
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success() is False
        assert str(result.exception) == expected_msg

    @pytest.mark.parametrize(
        "stage,expected_msg",
        [
            pytest.param(Stage.OFF, "old write", id="off"),
            pytest.param(Stage.DUALWRITE, "old write", id="dualwrite"),
            pytest.param(Stage.SHADOW, "old write", id="shadow"),
            pytest.param(Stage.LIVE, "new write", id="live"),
            pytest.param(Stage.RAMPDOWN, "new write", id="rampdown"),
            pytest.param(Stage.COMPLETE, "new write", id="complete"),
        ],
    )
    async def test_exception_in_authoritative_write(self, builder: AsyncMigratorBuilder, stage: Stage, expected_msg: str):
        builder.write(raises_exception("old write"), raises_exception("new write"))
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success() is False
        assert str(result.authoritative.exception) == expected_msg
        assert result.nonauthoritative is None

    @pytest.mark.parametrize(
        "stage,expected_msg,fail_old",
        [
            pytest.param(Stage.DUALWRITE, "new write", False, id="dualwrite"),
            pytest.param(Stage.SHADOW, "new write", False, id="shadow"),
            pytest.param(Stage.LIVE, "old write", True, id="live"),
            pytest.param(Stage.RAMPDOWN, "old write", True, id="rampdown"),
        ],
    )
    async def test_exception_in_nonauthoritative_write(self, builder: AsyncMigratorBuilder, stage: Stage, expected_msg: str, fail_old: bool):
        old_fn = raises_exception("old write") if fail_old else async_success
        new_fn = async_success if fail_old else raises_exception("new write")

        builder.write(old_fn, new_fn)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        result = await migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        assert result.nonauthoritative is not None
        assert not result.nonauthoritative.is_success()
        assert str(result.nonauthoritative.exception) == expected_msg


# ---------------------------------------------------------------------------
# Execution order (parallel via asyncio.gather vs serial)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSupportsExecutionOrder:
    @pytest.mark.parametrize(
        "order,min_time",
        [
            pytest.param(ExecutionOrder.PARALLEL, 300, id="parallel"),
            pytest.param(ExecutionOrder.SERIAL, 600, id="serial"),
            pytest.param(ExecutionOrder.RANDOM, 600, id="random"),
        ],
    )
    async def test_parallel(self, builder: AsyncMigratorBuilder, order: ExecutionOrder, min_time: int):
        async def delay(payload):
            await asyncio.sleep(0.3)
            return Result.success("success")

        builder.read_execution_order(order)
        builder.read(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, AsyncMigrator)

        start = datetime.now()
        result = await migrator.read('live', user, Stage.LIVE)
        delta = datetime.now() - start
        ms = timedelta_millis(delta)

        assert result.is_success()
        assert ms >= min_time
