"""
Tests that migration operation latency is measured on the monotonic clock.
"""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from ldclient.impl.util import Result
from ldclient.migrations import async_migrator as async_migrator_module
from ldclient.migrations import migrator as migrator_module
from ldclient.migrations.types import Origin


class _FakeMonotonic:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


def test_latency_is_measured_on_the_monotonic_clock(monkeypatch):
    clock = _FakeMonotonic()
    monkeypatch.setattr(migrator_module, "monotonic_seconds", clock)
    tracker = MagicMock()

    def fn(payload):
        # Simulates 42s of work; a wall-clock jump during the operation used to
        # corrupt the reported latency (datetime.now() is DST-bearing local time).
        clock.now = 1042.0
        return Result.success(None)

    executor = migrator_module.Executor(Origin.OLD, fn, tracker, True, False, None)
    executor.run()

    tracker.latency.assert_called_once_with(Origin.OLD, timedelta(seconds=42.0))


@pytest.mark.asyncio
async def test_async_latency_is_measured_on_the_monotonic_clock(monkeypatch):
    clock = _FakeMonotonic()
    monkeypatch.setattr(async_migrator_module, "monotonic_seconds", clock)
    tracker = MagicMock()

    async def fn(payload):
        clock.now = 1042.0
        return Result.success(None)

    executor = async_migrator_module.AsyncExecutor(Origin.OLD, fn, tracker, True, False, None)
    await executor.run()

    tracker.latency.assert_called_once_with(Origin.OLD, timedelta(seconds=42.0))
