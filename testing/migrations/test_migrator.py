import pytest
from datetime import datetime, timedelta
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.migrations import MigratorBuilder
from ldclient import Result
from ldclient.migrations.types import Stage, Origin, MigratorFn, ExecutionOrder
from ldclient.migrations.migrator import Migrator
from ldclient.versioned_data_kind import FEATURES
from testing.builders import FlagBuilder
from testing.test_ldclient import make_client, user
from typing import List
from time import sleep


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
    @pytest.mark.skip("cannot finish until we have migration op event")
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
        # TODO: Add tests that ensure each of the provided origins are in the invoked measurement

    @pytest.mark.skip("cannot finish until we have migration op event")
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
        # TODO: Add tests that ensure each of the provided origins are in the invoked measurement


class TestTrackingLatency:
    @pytest.mark.skip("cannot finish until we have migration op event")
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

        builder.read(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        # TODO: Add tests that ensure each of the provided origins are in the
        # latency measurement. We should also make sure the reported latency is
        # >= the 0.1 we are sleeping for.

    @pytest.mark.skip("cannot finish until we have migration op event")
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

        builder.write(delay, delay)
        migrator = builder.build()
        assert isinstance(migrator, Migrator)

        result = migrator.read(stage.value, user, Stage.LIVE)

        assert result.is_success()
        # TODO: Add tests that ensure each of the provided origins are in the
        # latency measurement. We should also make sure the reported latency is
        # >= the 0.1 we are sleeping for.


class TestTrackingErrors:
    @pytest.mark.skip("cannot finish until we have migration op event")
    def test_errors(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        # TODO: Add tests similar to the invoked tracking
        pass


class TestTrackingConsistency:
    @pytest.mark.skip("cannot finish until we have migration op event")
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
    def test_consistency(self, builder: MigratorBuilder, stage: Stage, origins: List[Origin]):
        # TODO: Add tests that check when it is the same, different, and when
        # it throws an exception.
        pass


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
        ms = delta / timedelta(milliseconds=1)

        assert result.is_success()
        assert ms >= min_time
