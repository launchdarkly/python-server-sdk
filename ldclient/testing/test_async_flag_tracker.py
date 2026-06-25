"""
Tests for AsyncFlagValueChangeListener and AsyncFlagTrackerImpl.
"""
import asyncio
import threading

import pytest
import pytest_asyncio

from ldclient.context import Context
from ldclient.impl.async_flag_tracker import (
    AsyncFlagTrackerImpl,
    AsyncFlagValueChangeListener
)
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import FlagChange, FlagValueChange

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def context():
    return Context.create('user-1')


@pytest.fixture
def listeners():
    return Listeners()


@pytest.fixture
def eval_values():
    """Mutable container so tests can change the value returned by eval_fn."""
    return {'value': 'initial'}


@pytest.fixture
def eval_fn(eval_values):
    async def _fn(key, ctx):
        return eval_values['value']
    return _fn


@pytest_asyncio.fixture
async def tracker(listeners, eval_fn):
    # The tracker's scheduler captures the running event loop at construction.
    return AsyncFlagTrackerImpl(listeners, eval_fn)


# ---------------------------------------------------------------------------
# AsyncFlagValueChangeListener tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_initializes_value_without_notifying(tracker, context, eval_fn):
    changes = []
    await tracker.add_flag_value_change_listener('flag-key', context, changes.append)

    # No notification on creation — only records the baseline value
    assert changes == []


@pytest.mark.asyncio
async def test_flag_change_matching_key_triggers_listener(tracker, listeners, eval_values, context):
    received = []
    await tracker.add_flag_value_change_listener('flag-key', context, received.append)

    # Change the value the eval_fn returns
    eval_values['value'] = 'changed'

    listeners.notify(FlagChange('flag-key'))
    await asyncio.sleep(0.1)

    assert len(received) == 1
    change = received[0]
    assert change.key == 'flag-key'
    assert change.old_value == 'initial'
    assert change.new_value == 'changed'


@pytest.mark.asyncio
async def test_flag_change_same_value_does_not_trigger_listener(tracker, listeners, context):
    received = []
    await tracker.add_flag_value_change_listener('flag-key', context, received.append)

    # Value stays the same
    listeners.notify(FlagChange('flag-key'))
    await asyncio.sleep(0.1)

    assert received == []


@pytest.mark.asyncio
async def test_flag_change_non_matching_key_ignored(tracker, listeners, eval_values, context):
    received = []
    await tracker.add_flag_value_change_listener('flag-key', context, received.append)

    eval_values['value'] = 'changed'

    # Different flag key — should be ignored
    listeners.notify(FlagChange('other-flag'))
    await asyncio.sleep(0.1)

    assert received == []


@pytest.mark.asyncio
async def test_listener_callback_is_sync(tracker, listeners, eval_values, context):
    """The listener callback is invoked synchronously (not awaited)."""
    was_called = []

    def sync_listener(change: FlagValueChange):
        was_called.append(change)

    await tracker.add_flag_value_change_listener('flag-key', context, sync_listener)

    eval_values['value'] = 'new'
    listeners.notify(FlagChange('flag-key'))
    await asyncio.sleep(0.1)

    assert len(was_called) == 1


# ---------------------------------------------------------------------------
# AsyncFlagTrackerImpl tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_flag_value_change_listener_returns_listener(tracker, context):
    listener = await tracker.add_flag_value_change_listener('flag-key', context, lambda c: None)
    assert isinstance(listener, AsyncFlagValueChangeListener)


@pytest.mark.asyncio
async def test_add_listener(tracker, listeners):
    received = []

    def listener(change: FlagChange):
        received.append(change.key)

    tracker.add_listener(listener)
    listeners.notify(FlagChange('my-flag'))

    assert received == ['my-flag']


@pytest.mark.asyncio
async def test_remove_listener(tracker, listeners):
    received = []

    def listener(change: FlagChange):
        received.append(change.key)

    tracker.add_listener(listener)
    tracker.remove_listener(listener)

    listeners.notify(FlagChange('my-flag'))

    assert received == []


@pytest.mark.asyncio
async def test_remove_flag_value_change_listener(tracker, listeners, eval_values, context):
    received = []
    listener = await tracker.add_flag_value_change_listener('flag-key', context, received.append)

    tracker.remove_listener(listener)

    eval_values['value'] = 'updated'
    listeners.notify(FlagChange('flag-key'))
    await asyncio.sleep(0.1)

    # After removal, no notification should fire
    assert received == []


@pytest.mark.asyncio
async def test_notify_from_worker_thread_fires_listener(tracker, listeners, eval_values, context):
    """Regression: notify() called from a background thread (no running event loop in
    that thread) must still schedule the evaluation correctly and invoke the listener.

    asyncio.create_task raises RuntimeError when called from a non-event-loop thread.
    The scheduler uses run_coroutine_threadsafe, which works from any thread.
    """
    received = []
    await tracker.add_flag_value_change_listener('flag-key', context, received.append)

    eval_values['value'] = 'changed-from-thread'

    def notify_from_thread():
        # This thread has no running event loop — verifies run_coroutine_threadsafe is used
        listeners.notify(FlagChange('flag-key'))

    t = threading.Thread(target=notify_from_thread)
    t.start()
    t.join()

    # Allow the scheduled coroutine to run on the event loop
    await asyncio.sleep(0.1)

    assert len(received) == 1
    assert received[0].new_value == 'changed-from-thread'
