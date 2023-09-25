import pytest
from datetime import timedelta
from ldclient.migrations import OpTracker, Stage, Operation, Origin
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.types import EventInputMigrationOp
from testing.builders import build_off_flag_with_value
from testing.test_ldclient import user


@pytest.fixture
def bare_tracker() -> OpTracker:
    flag = build_off_flag_with_value("flag", True).build()
    detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
    tracker = OpTracker(flag, user, detail, Stage.LIVE)

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
        assert isinstance(event, EventInputMigrationOp)

    def test_fails_without_operation(self, bare_tracker: OpTracker):
        event = bare_tracker.build()

        assert isinstance(event, str)
        assert event == "operation not provided"

    def test_fails_without_flag(self):
        detail = EvaluationDetail('value', 0, {'kind': 'OFF'})
        tracker = OpTracker(None, user, detail, Stage.LIVE)
        tracker.operation(Operation.WRITE)
        event = tracker.build()

        assert isinstance(event, str)
        assert event == "flag not provided"

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
        invalid_context = {"kind": "multi", "key": "user-key"}
        tracker = OpTracker(flag, invalid_context, detail, Stage.LIVE)
        tracker.operation(Operation.WRITE)
        tracker.invoked(Origin.OLD)
        event = tracker.build()

        assert isinstance(event, str)
        assert event == "provided context was invalid"

    @pytest.mark.parametrize(
        "invoked,recorded",
        [
            pytest.param(Origin.OLD, Origin.NEW, id="invoked old measured new"),
            pytest.param(Origin.OLD, Origin.NEW, id="invoked new measured old"),
        ],
    )
    def test_latency_invoked_mismatch(
            self, bare_tracker: OpTracker, invoked: Origin, recorded: Origin):
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
            pytest.param(Origin.OLD, Origin.NEW, id="invoked new measured old"),
        ],
    )
    def test_error_invoked_mismatch(
            self, bare_tracker: OpTracker, invoked: Origin, recorded: Origin):
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
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.invoked) == 1
        assert origin in event.invoked

    def test_tracks_both(self, bare_tracker: OpTracker):
        bare_tracker.operation(Operation.WRITE)
        bare_tracker.invoked(Origin.OLD)
        bare_tracker.invoked(Origin.NEW)

        event = bare_tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.invoked) == 2
        assert Origin.OLD in event.invoked
        assert Origin.NEW in event.invoked

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.invoked("this is clearly wrong")  # type: ignore[arg-type]
        tracker.invoked(False)  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.invoked) == 2
        assert Origin.OLD in event.invoked
        assert Origin.NEW in event.invoked


class TestTrackConsistency:
    @pytest.mark.parametrize(
        "consistent",
        [True, False],
    )
    def test_without_sampling_ratio(
            self, tracker: OpTracker, consistent: bool):
        tracker.consistent(lambda: consistent)
        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert event.consistent is consistent
        assert event.consistent_ratio is None

    @pytest.mark.skip(reason="sampling ratio is not yet supported")
    def test_with_sampling_ratio_of_1(
            self, tracker: OpTracker, consistent: bool):
        # TODO(sampling-ratio): Fill in test
        pass

    @pytest.mark.skip(reason="sampling ratio is not yet supported")
    def test_can_disable_with_sampling_ratio_of_0(
            self, tracker: OpTracker, consistent: bool):
        # TODO(sampling-ratio): Fill in test
        pass

    @pytest.mark.skip(reason="sampling ratio is not yet supported")
    def test_with_non_trivial_sampling_ratio(
            self, tracker: OpTracker, consistent: bool):
        # TODO(sampling-ratio): Fill in test
        pass


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
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.errors) == 1
        assert origin in event.errors

    def test_tracks_both(self, tracker: OpTracker):
        tracker.operation(Operation.WRITE)
        tracker.error(Origin.OLD)
        tracker.error(Origin.NEW)

        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.errors) == 2
        assert Origin.OLD in event.errors
        assert Origin.NEW in event.errors

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.error("this is clearly wrong")  # type: ignore[arg-type]
        tracker.error(False)  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

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
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.latencies) == 1
        assert event.latencies[origin] == timedelta(milliseconds=10)

    def test_tracks_both(self, tracker: OpTracker):
        tracker.operation(Operation.WRITE)
        tracker.latency(Origin.OLD, timedelta(milliseconds=10))
        tracker.latency(Origin.NEW, timedelta(milliseconds=5))

        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.latencies) == 2
        assert event.latencies[Origin.OLD] == timedelta(milliseconds=10)
        assert event.latencies[Origin.NEW] == timedelta(milliseconds=5)

    def test_ignores_invalid_origins(self, tracker: OpTracker):
        tracker.latency("this is clearly wrong", timedelta(milliseconds=10))  # type: ignore[arg-type]
        tracker.latency(False, timedelta(milliseconds=5))  # type: ignore[arg-type]

        event = tracker.build()
        assert isinstance(event, EventInputMigrationOp)

        assert len(event.latencies) == 0
