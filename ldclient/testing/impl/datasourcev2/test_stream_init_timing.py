"""
Stream-init timing is measured on the monotonic clock while the reported
timestamp stays on the wall clock. These tests exercise ``_record_stream_init``
in isolation so the clock source is observable without driving a live
connection. They assert the clock behavior only; the recording semantics
(projected baseline, negative-elapsed clamp, skip-when-cleared) are unchanged
from the wall-clock implementation.
"""

from unittest.mock import Mock

import pytest

from ldclient.impl.datasourcev2 import async_streaming, streaming
from ldclient.impl.datasourcev2.async_streaming import AsyncStreamingDataSource
from ldclient.impl.datasourcev2.streaming import StreamingDataSource

_CASES = [
    pytest.param(streaming, StreamingDataSource, id="sync"),
    pytest.param(async_streaming, AsyncStreamingDataSource, id="async"),
]


def _make_source(cls, baseline):
    # Bypass __init__ so the test depends only on the two attributes
    # _record_stream_init reads.
    source = cls.__new__(cls)
    source._diagnostic_accumulator = Mock()
    source._connection_attempt_start_time = baseline
    return source


@pytest.mark.parametrize("module, cls", _CASES)
def test_duration_measured_on_monotonic_clock_timestamp_on_wall_clock(monkeypatch, module, cls):
    monotonic = {"t": 100.0}
    wall_millis = {"t": 5_000_000}
    monkeypatch.setattr(module, "monotonic_seconds", lambda: monotonic["t"])
    monkeypatch.setattr(module, "current_time_millis", lambda: wall_millis["t"])

    source = _make_source(cls, monotonic["t"])

    # The monotonic clock advances two seconds; the wall clock jumps backward an
    # hour (an NTP correction). The duration must reflect the monotonic advance,
    # and the reported timestamp must reflect the (jumped) wall clock.
    monotonic["t"] += 2.0
    wall_millis["t"] -= 3_600_000

    source._record_stream_init(False)

    source._diagnostic_accumulator.record_stream_init.assert_called_once_with(
        wall_millis["t"], 2000, False
    )


@pytest.mark.parametrize("module, cls", _CASES)
def test_negative_elapsed_is_clamped_to_zero(monkeypatch, module, cls):
    monotonic = {"t": 100.0}
    wall_millis = {"t": 5_000_000}
    monkeypatch.setattr(module, "monotonic_seconds", lambda: monotonic["t"])
    monkeypatch.setattr(module, "current_time_millis", lambda: wall_millis["t"])

    # The baseline is a projected future attempt start, so the measured elapsed
    # is negative and must clamp to zero (unchanged from the wall-clock code).
    source = _make_source(cls, monotonic["t"] + 3.0)

    source._record_stream_init(True)

    source._diagnostic_accumulator.record_stream_init.assert_called_once_with(
        wall_millis["t"], 0, True
    )


@pytest.mark.parametrize("module, cls", _CASES)
def test_nothing_recorded_when_baseline_is_cleared(monkeypatch, module, cls):
    monkeypatch.setattr(module, "monotonic_seconds", lambda: 100.0)
    monkeypatch.setattr(module, "current_time_millis", lambda: 5_000_000)

    source = _make_source(cls, None)

    source._record_stream_init(False)

    source._diagnostic_accumulator.record_stream_init.assert_not_called()
