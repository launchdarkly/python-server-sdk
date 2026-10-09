"""
Tests for the FDv2 fallback/recovery conditions and the monotonic
time-in-state measurement in :class:`_StateAgeTracker`.
"""

import time

import pytest

from ldclient.impl.datasystem import fdv2_common
from ldclient.impl.datasystem.fdv2_common import (
    _StateAgeTracker,
    fallback_condition,
    recovery_condition
)
from ldclient.interfaces import DataSourceState, DataSourceStatus


def _status(state: DataSourceState, since: float = 0.0) -> DataSourceStatus:
    return DataSourceStatus(state, since or time.time(), None)


class _FakeMonotonic:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def mono(monkeypatch):
    clock = _FakeMonotonic()
    monkeypatch.setattr(fdv2_common, "monotonic_seconds", clock)
    return clock


@pytest.mark.parametrize(
    "state,seconds,expected",
    [
        (DataSourceState.INTERRUPTED, 59, False),
        (DataSourceState.INTERRUPTED, 60, False),  # strictly greater than
        (DataSourceState.INTERRUPTED, 61, True),
        (DataSourceState.INITIALIZING, 9, False),
        (DataSourceState.INITIALIZING, 10, False),  # strictly greater than
        (DataSourceState.INITIALIZING, 11, True),
        (DataSourceState.VALID, 10_000, False),
        (DataSourceState.OFF, 10_000, False),
    ],
)
def test_fallback_condition(state, seconds, expected):
    assert fallback_condition(_status(state), seconds) is expected


@pytest.mark.parametrize(
    "state,seconds,expected",
    [
        (DataSourceState.VALID, 299, False),
        (DataSourceState.VALID, 300, False),  # strictly greater than
        (DataSourceState.VALID, 301, True),
        (DataSourceState.INTERRUPTED, 10_000, False),
        (DataSourceState.INITIALIZING, 10_000, False),
        (DataSourceState.OFF, 10_000, False),
    ],
)
def test_recovery_condition(state, seconds, expected):
    assert recovery_condition(_status(state), seconds) is expected


def test_tracker_measures_age_from_first_observation(mono):
    tracker = _StateAgeTracker()
    status = _status(DataSourceState.INTERRUPTED, since=500.0)

    assert tracker.seconds_in_state(status) == 0.0

    mono.now = 1030.0
    assert tracker.seconds_in_state(status) == 30.0


def test_tracker_keeps_age_across_same_state_error_updates(mono):
    tracker = _StateAgeTracker()
    # Same (state, since), new object: how a same-state error update looks.
    first = _status(DataSourceState.INTERRUPTED, since=500.0)
    second = _status(DataSourceState.INTERRUPTED, since=500.0)

    tracker.seconds_in_state(first)
    mono.now = 1030.0
    assert tracker.seconds_in_state(second) == 30.0


def test_tracker_resets_on_state_change(mono):
    tracker = _StateAgeTracker()
    tracker.seconds_in_state(_status(DataSourceState.INITIALIZING, since=500.0))

    mono.now = 1030.0
    assert tracker.seconds_in_state(_status(DataSourceState.VALID, since=530.0)) == 0.0

    mono.now = 1045.0
    assert tracker.seconds_in_state(_status(DataSourceState.VALID, since=530.0)) == 15.0


def test_tracker_resets_on_a_flap_through_another_state(mono):
    tracker = _StateAgeTracker()
    # INTERRUPTED -> VALID -> INTERRUPTED between two samples: the state
    # matches the last sample but ``since`` differs, so the age must reset.
    tracker.seconds_in_state(_status(DataSourceState.INTERRUPTED, since=500.0))

    mono.now = 1070.0
    assert tracker.seconds_in_state(_status(DataSourceState.INTERRUPTED, since=560.0)) == 0.0


def test_wall_clock_step_cannot_trigger_a_spurious_fallback(monkeypatch):
    tracker = _StateAgeTracker()
    status = DataSourceStatus(DataSourceState.INTERRUPTED, time.time(), None)

    real_time = time.time()
    # A forward wall step used to make the interrupted state look an hour old,
    # which would have triggered an immediate synchronizer fallback.
    monkeypatch.setattr(time, "time", lambda: real_time + 3600)

    seconds = tracker.seconds_in_state(status)
    assert fallback_condition(status, seconds) is False
