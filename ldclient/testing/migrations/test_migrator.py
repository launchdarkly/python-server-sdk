from datetime import datetime, timedelta
from time import sleep
from typing import List

import pytest

from ldclient import Result
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.util import timedelta_millis
from ldclient.migrations import MigratorBuilder
from ldclient.migrations.migrator import Migrator
from ldclient.migrations.tracker import MigrationOpEvent
from ldclient.migrations.types import ExecutionOrder, MigratorFn, Origin, Stage
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.test_ldclient import make_client, user
from ldclient.versioned_data_kind import FEATURES


def success(payload) -> Result:
    return Result.success(True)


def raises_exception(msg) -> MigratorFn:
    """Quick helper to generate a migration fn that is going to raise an exception"""

    def inner(payload):
        raise Exception(msg)

    return inner


@pytest.fixture
def data_store():
    flags = {}
    for stage in Stage:
        feature = FlagBuilder(stage.value).on(True).variations(stage.value).fallthrough_variation(0).build()
        flags[stage.value] = feature

    store = InMemoryFeatureStore()
    store.init({FEATURES: flags})

    return store


@pytest.fixture
def builder(data_store) -> MigratorBuilder:
    client = make_client(data_store)
    builder = MigratorBuilder(client)
    builder.track_latency(False)
    builder.track_errors(False)

    builder.read(success, success, None)
    builder.write(success, success)

    return builder


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
    def test_passes_through_read(self, builder: MigratorBuilder, stage: Stage, count: int):
        payloads = []

        def capture_payloads(payload):
            payloads.append(payload)
            return Result.success(None)

        builder.read(capture_payloads, capture_payloads)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE, "payload")

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
    def test_passes_through_write(self, builder: MigratorBuilder, stage: Stage, count: int):
        payloads = []

        def capture_payloads(payload):
            payloads.append(payload)
            return Result.success(None)

        builder.write(capture_payloads, capture_payloads)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE, "payload")

        assert result.authoritative.is_success()
        if result.nonauthoritative is not None:
            assert result.nonauthoritative.is_success()

        assert len(payloads) == count
        assert all("payload" == p for p in payloads)


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
    def test_reads(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
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
    def test_writes(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.invoked)
        assert all(o in event.invoked for o in origins)


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
    def test_reads(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        def delay(payload):
            sleep(0.1)
            return Result.success("success")

        builder.track_latency(True)
        builder.read(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
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
    def test_writes(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        def delay(payload):
            sleep(0.1)
            return Result.success("success")

        builder.track_latency(True)
        builder.write(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert len(origins) == len(event.latencies)
        for o in origins:
            assert o in event.latencies
            assert event.latencies[o] >= timedelta(milliseconds=100)


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
    def test_reads(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        builder.track_errors(True)
        builder.read(lambda _: Result.fail("fail"), lambda _: Result.fail("fail"))
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

        assert not result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
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
    def test_authoritative_writes(self, builder: MigratorBuilder, stage: Stage, origin: Origin):
        builder.track_errors(True)
        builder.write(lambda _: Result.fail("fail"), lambda _: Result.fail("fail"))
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert not result.authoritative.is_success()
        assert result.nonauthoritative is None
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert 1 == len(event.errors)
        assert origin in event.errors

    @pytest.mark.parametrize(
        "stage,fail_old,fail_new,origin",
        [
            # Skip OFF and COMPLETE since they don't have non-authoritative writes
            pytest.param(Stage.DUALWRITE, False, True, Origin.NEW, id="dualwrite"),
            pytest.param(Stage.SHADOW, False, True, Origin.NEW, id="shadow"),
            pytest.param(Stage.LIVE, True, False, Origin.OLD, id="live"),
            pytest.param(Stage.RAMPDOWN, True, False, Origin.OLD, id="rampdown"),
        ],
    )
    def test_nonauthoritative_writes(self, builder: MigratorBuilder, stage: Stage, fail_old: bool, fail_new: bool, origin: Origin):
        def success(_):
            return Result.success(None)

        def fail(_):
            return Result.fail("fail")

        builder.track_errors(True)
        builder.write(fail if fail_old else success, fail if fail_new else success)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        assert result.nonauthoritative is not None
        assert not result.nonauthoritative.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert 1 == len(event.errors)
        assert origin in event.errors


class TestTrackingConsistency:
    @pytest.mark.parametrize(
        "stage",
        [
            pytest.param(Stage.OFF, id="off"),
            pytest.param(Stage.DUALWRITE, id="dualwrite"),
            # SHADOW and LIVE are tested separately since they actually trigger consistency checks.
            pytest.param(Stage.RAMPDOWN, id="rampdown"),
            pytest.param(Stage.COMPLETE, id="complete"),
        ],
    )
    def test_consistency_is_not_run_in_most_stages(self, builder: MigratorBuilder, stage: Stage):
        builder.read(lambda _: Result.success("value"), lambda _: Result.success("value"), lambda lhs, rhs: lhs == rhs)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is None

    @pytest.mark.parametrize(
        "stage,old,new,expected",
        [
            # SHADOW and LIVE are the only two stages that run both origins for read.
            pytest.param(Stage.SHADOW, "value", "value", True, id="shadow matches"),
            pytest.param(Stage.LIVE, "value", "value", True, id="live matches"),
            pytest.param(Stage.SHADOW, "old", "new", False, id="shadow does not match"),
            pytest.param(Stage.LIVE, "old", "new", False, id="live does not match"),
        ],
    )
    def test_consistency_is_tracked_correctly(self, builder: MigratorBuilder, stage: Stage, old: str, new: str, expected: bool):
        builder.read(lambda _: Result.success(old), lambda _: Result.success(new), lambda lhs, rhs: lhs == rhs)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is expected

    @pytest.mark.parametrize(
        "stage,old,new,expected",
        [
            # SHADOW and LIVE are the only two stages that run both origins for read.
            pytest.param(Stage.SHADOW, "value", "value", True, id="shadow matches"),
            pytest.param(Stage.LIVE, "value", "value", True, id="live matches"),
            pytest.param(Stage.SHADOW, "old", "new", False, id="shadow does not match"),
            pytest.param(Stage.LIVE, "old", "new", False, id="live does not match"),
        ],
    )
    def test_consistency_handles_exceptions(self, builder: MigratorBuilder, stage: Stage, old: str, new: str, expected: bool):
        def raise_exception(lhs, rhs):
            raise Exception("error")

        builder.read(lambda _: Result.success(old), lambda _: Result.success(new), raise_exception)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)
        assert result.is_success()
        events = builder._client._event_processor._events  # type: ignore
        assert isinstance(events[0], EventInputEvaluation)

        event = events[1]
        assert isinstance(event, MigrationOpEvent)
        assert event.consistent is None


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
    def test_reads(self, builder: MigratorBuilder, stage: Stage, expected_msg: str):

        builder.read(raises_exception("old read"), raises_exception("new read"))
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

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
    def test_exception_in_authoritative_write(self, builder: MigratorBuilder, stage: Stage, expected_msg: str):

        builder.write(raises_exception("old write"), raises_exception("new write"))
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success() is False
        assert str(result.authoritative.exception) == expected_msg
        assert result.nonauthoritative is None

    @pytest.mark.parametrize(
        "stage,expected_msg,old_fn,new_fn",
        [
            # Skip OFF and COMPLETE since they don't have non-authoritative writes
            pytest.param(Stage.DUALWRITE, "new write", success, raises_exception("new write"), id="dualwrite"),
            pytest.param(Stage.SHADOW, "new write", success, raises_exception("new write"), id="shadow"),
            pytest.param(Stage.LIVE, "old write", raises_exception("old write"), success, id="live"),
            pytest.param(Stage.RAMPDOWN, "old write", raises_exception("old write"), success, id="rampdown"),
        ],
    )
    def test_exception_in_nonauthoritative_write(self, builder: MigratorBuilder, stage: Stage, expected_msg: str, old_fn: MigratorFn, new_fn: MigratorFn):

        builder.write(old_fn, new_fn)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.write(stage.value, user, Stage.LIVE)

        assert result.authoritative.is_success()
        assert result.nonauthoritative is not None
        assert not result.nonauthoritative.is_success()
        assert str(result.nonauthoritative.exception) == expected_msg


class TestSupportsExectionOrder:
    @pytest.mark.parametrize(
        "order,min_time",
        [
            pytest.param(ExecutionOrder.PARALLEL, 300, id="parallel"),
            pytest.param(ExecutionOrder.SERIAL, 600, id="serial"),
            pytest.param(ExecutionOrder.RANDOM, 600, id="random"),
        ],
    )
    def test_parallel(self, builder: MigratorBuilder, order: ExecutionOrder, min_time: int):
        def delay(payload):
            sleep(0.3)
            return Result.success("success")

        builder.read_execution_order(order)
        builder.read(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        start = datetime.now()
        result = migrator.read('live', user, Stage.LIVE)
        delta = datetime.now() - start
        ms = timedelta_millis(delta)

        assert result.is_success()
        assert ms >= min_time
