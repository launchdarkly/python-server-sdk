"""
Tests for the monotonic stream-init timing in the FDv2 streaming synchronizers.
"""

import asyncio
import time

from ldclient.impl.datasourcev2 import (
    async_streaming as async_streaming_module
)
from ldclient.impl.datasourcev2 import streaming as streaming_module
from ldclient.impl.datasourcev2.async_streaming import AsyncStreamingDataSource
from ldclient.impl.datasourcev2.streaming import StreamingDataSource
from ldclient.impl.events.diagnostics import _DiagnosticAccumulator


class _Recorder:
    def __init__(self):
        self.recorded = []

    def record_stream_init(self, timestamp, duration, failed):
        self.recorded.append((timestamp, duration, failed))


class _FakeMonotonic:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


def _bare(cls):
    proc = object.__new__(cls)
    recorder = _Recorder()
    proc._diagnostic_accumulator = recorder
    return proc, recorder


def test_duration_unaffected_by_forward_wall_step(monkeypatch):
    # A forward wall step used to inflate the reported duration by the step size.
    for cls in (StreamingDataSource, AsyncStreamingDataSource):
        proc, recorder = _bare(cls)
        proc._connection_attempt_started_monotonic = time.monotonic()

        real_time = time.time()
        monkeypatch.setattr(time, "time", lambda: real_time + 3600)
        proc._record_stream_init(False)
        monkeypatch.undo()

        _, duration, failed = recorder.recorded[0]
        assert failed is False
        assert duration >= 0
        assert duration < 1000


def test_backward_wall_step_does_not_zero_the_measurement(monkeypatch):
    # A backward wall step used to drive the elapsed value negative, and the old
    # clamp silently reported 0 instead of the real measurement.
    for cls, module in (
        (StreamingDataSource, streaming_module),
        (AsyncStreamingDataSource, async_streaming_module),
    ):
        proc, recorder = _bare(cls)
        clock = _FakeMonotonic(1000.0)
        monkeypatch.setattr(module, "monotonic_seconds", clock)
        proc._connection_attempt_started_monotonic = clock()
        clock.now = 1000.5

        real_time = time.time()
        monkeypatch.setattr(time, "time", lambda: real_time - 120)
        proc._record_stream_init(True)
        monkeypatch.undo()

        _, duration, failed = recorder.recorded[0]
        assert failed is True
        assert duration == 500


def test_error_handling_clears_the_baseline():
    # The projection was removed: _handle_error no longer fabricates the next
    # attempt's start (the library owns the reconnect sleep, so its real start
    # is not visible here). It clears the baseline; the next Start re-stamps it.
    proc = object.__new__(StreamingDataSource)
    proc._running = True
    proc._diagnostic_accumulator = None
    proc._connection_attempt_started_monotonic = 1234.0

    update, should_continue = proc._handle_error(Exception("boom"), None)

    assert should_continue is True
    assert update is not None
    assert proc._connection_attempt_started_monotonic is None


def test_async_error_handling_clears_the_baseline():
    proc = object.__new__(AsyncStreamingDataSource)
    proc._running = True
    proc._diagnostic_accumulator = None
    proc._connection_attempt_started_monotonic = 1234.0

    update, should_continue = asyncio.run(proc._handle_error(Exception("boom"), None))

    assert should_continue is True
    assert update is not None
    assert proc._connection_attempt_started_monotonic is None


def test_diagnostic_accumulator_clamps_negative_durations():
    # The diagnostic schema requires durationMillis >= 0; the accumulator
    # enforces it once for every call site.
    accumulator = _DiagnosticAccumulator("diag-id")
    accumulator.record_stream_init(123, -5, False)

    assert accumulator._stream_inits[0]['durationMillis'] == 0
