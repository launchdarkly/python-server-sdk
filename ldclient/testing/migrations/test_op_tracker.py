from datetime import timedelta

import pytest

from ldclient import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.migrations import (MigrationOpEvent, Operation, OpTracker,
                                 Origin, Stage)
from ldclient.testing.builders import (MigrationSettingsBuilder,
                                       build_off_flag_with_value)
from ldclient.testing.test_ldclient import user


@pytest.fixture
def bare_tracker() -> OpTracker:
    flag = build_off_flag_with_value("flag", True).build()
    detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
    tracker = OpTracker("flag", flag, user, detail, Stage.LIVE)

    return tracker


@pytest.fixture
def tracker(bare_tracker) -> OpTracker:
    bare_tracker.operation(Operation.READ)
    bare_tracker.invoked(Origin.OLD)
    bare_tracker.invoked(Origin.NEW)

    return bare_tracker


class TestBuilding:
    def test_can_build_successfully(self, tracker: OpTracker):
        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

    def test_can_build_successfully_without_a_flag(self):
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        tracker = OpTracker("flag", None, user, detail, Stage.LIVE)
        tracker.operation(Operation.READ)
        tracker.invoked(Origin.OLD)

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

    def test_fails_without_operation(self, bare_tracker: OpTracker):
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == "operation not provided"

    def test_fails_with_empty_key(self):
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        flag = build_off_flag_with_value("flag", True).build()
        tracker = OpTracker("", flag, user, detail, Stage.LIVE)
        tracker.operation(Operation.WRITE)
        event = tracker.build()

        assert isinstance(event, str)
        assert event == "migration operation cannot contain an empty key"

    def test_fails_with_invalid_operation(self, bare_tracker: OpTracker):
        bare_tracker.operation("invalid operation")  # type: ignore[arg-type]
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == "operation not provided"

    def test_fails_without_invocations(self, bare_tracker: OpTracker):
        bare_tracker.operation(Operation.WRITE)
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == "no origins were invoked"

    def test_with_invalid_context(self):
        flag = build_off_flag_with_value("flag", True).build()
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        invalid_context = Context.from_dict({"kind": "multi", "key": "user-key"})
        tracker = OpTracker("flag", flag, invalid_context, detail, Stage.LIVE)
        tracker.operation(Operation.WRITE)
        tracker.invoked(Origin.OLD)
        event = tracker.build()

        assert isinstance(event, str)
        assert event == "provided context was invalid"

    @pytest.mark.parametrize(
        "invoked,recorded",
        [
            pytest.param(Origin.OLD, Origin.NEW, id="invoked old measured new"),
            pytest.param(Origin.NEW, Origin.OLD, id="invoked new measured old"),
        ],
    )
    def test_latency_invoked_mismatch(self, bare_tracker: OpTracker, invoked: Origin, recorded: Origin):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(invoked)
        bare_tracker.latency(recorded, timedelta(milliseconds=20))
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == f"provided latency for origin '{recorded.value}' without recording invocation"

    @pytest.mark.parametrize(
        "invoked,recorded",
        [
            pytest.param(Origin.OLD, Origin.NEW, id="invoked old measured new"),
            pytest.param(Origin.NEW, Origin.OLD, id="invoked new measured old"),
        ],
    )
    def test_error_invoked_mismatch(self, bare_tracker: OpTracker, invoked: Origin, recorded: Origin):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(invoked)
        bare_tracker.error(recorded)
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == f"provided error for origin '{recorded.value}' without recording invocation"

    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param(Origin.OLD, id="old"),
            pytest.param(Origin.NEW, id="new"),
        ],
    )
    def test_consistency_invoked_mismatch(self, bare_tracker: OpTracker, origin: Origin):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(origin)
        bare_tracker.consistent(lambda: True)
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == "provided consistency without recording both invocations"


class TestTrackInvocations:
    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param(Origin.OLD, id="old"),
            pytest.param(Origin.NEW, id="new"),
        ],
    )
    def test_individually(self, bare_tracker: OpTracker, origin: Origin):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(origin)

        event = bare_tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.invoked) == 1
        assert origin in event.invoked

    def test_tracks_both(self, bare_tracker: OpTracker):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(Origin.OLD)
        bare_tracker.invoked(Origin.NEW)

        event = bare_tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.invoked) == 2
        assert Origin.OLD in event.invoked
        assert Origin.NEW in event.invoked

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.invoked("this is clearly wrong")  # type: ignore[arg-type]
        tracker.invoked(False)  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.invoked) == 2
        assert Origin.OLD in event.invoked
        assert Origin.NEW in event.invoked


class TestTrackConsistency:
    @pytest.mark.parametrize("consistent", [True, False])
    def test_without_check_ratio(self, tracker: OpTracker, consistent: bool):
        tracker.consistent(lambda: consistent)
        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert event.consistent is consistent
        assert event.consistent_ratio == 1

    @pytest.mark.parametrize("consistent", [True, False])
    def test_with_check_ratio_of_1(self, consistent):
        flag = build_off_flag_with_value("flag", 'off').migrations(MigrationSettingsBuilder().check_ratio(1).build()).build()
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        tracker = OpTracker("flag", flag, user, detail, Stage.LIVE)
        tracker.consistent(lambda: consistent)
        tracker.operation(Operation.READ)
        tracker.invoked(Origin.OLD)
        tracker.invoked(Origin.NEW)

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert event.consistent is consistent
        assert event.consistent_ratio == 1

    @pytest.mark.parametrize("consistent", [True, False])
    def test_can_disable_with_check_ratio_of_0(self, consistent: bool):
        flag = build_off_flag_with_value("flag", 'off').migrations(MigrationSettingsBuilder().check_ratio(0).build()).build()
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        tracker = OpTracker("flag", flag, user, detail, Stage.LIVE)
        tracker.consistent(lambda: consistent)
        tracker.operation(Operation.READ)
        tracker.invoked(Origin.OLD)
        tracker.invoked(Origin.NEW)

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert event.consistent is None
        assert event.consistent_ratio is None


class TestTrackErrors:
    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param(Origin.OLD, id="old"),
            pytest.param(Origin.NEW, id="new"),
        ],
    )
    def test_individually(self, tracker: OpTracker, origin: Origin):
        tracker.operation(Operation.WRITE)
        tracker.error(origin)

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.errors) == 1
        assert origin in event.errors

    def test_tracks_both(self, tracker: OpTracker):
        tracker.operation(Operation.WRITE)
        tracker.error(Origin.OLD)
        tracker.error(Origin.NEW)

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.errors) == 2
        assert Origin.OLD in event.errors
        assert Origin.NEW in event.errors

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.error("this is clearly wrong")  # type: ignore[arg-type]
        tracker.error(False)  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.errors) == 0


class TestTrackLatencies:
    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param(Origin.OLD, id="old"),
            pytest.param(Origin.NEW, id="new"),
        ],
    )
    def test_individually(self, tracker: OpTracker, origin: Origin):
        tracker.operation(Operation.WRITE)
        tracker.latency(origin, timedelta(milliseconds=10))

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.latencies) == 1
        assert event.latencies[origin] == timedelta(milliseconds=10)

    def test_tracks_both(self, tracker: OpTracker):
        tracker.operation(Operation.WRITE)
        tracker.latency(Origin.OLD, timedelta(milliseconds=10))
        tracker.latency(Origin.NEW, timedelta(milliseconds=5))

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.latencies) == 2
        assert event.latencies[Origin.OLD] == timedelta(milliseconds=10)
        assert event.latencies[Origin.NEW] == timedelta(milliseconds=5)

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.latency("this is clearly wrong", timedelta(milliseconds=10))  # type: ignore[arg-type]
        tracker.latency(False, timedelta(milliseconds=5))  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, MigrationOpEvent)

        assert len(event.latencies) == 0
