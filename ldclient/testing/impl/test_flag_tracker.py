from ldclient.impl.flag_tracker import FlagTrackerImpl
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import FlagChange
from ldclient.testing.test_util import SpyListener


def test_can_add_and_remove_listeners():
    spy = SpyListener()
    listeners = Listeners()

    tracker = FlagTrackerImpl(listeners, lambda: None)
    tracker.add_listener(spy)

    listeners.notify(FlagChange('flag-1'))
    listeners.notify(FlagChange('flag-2'))

    tracker.remove_listener(spy)

    listeners.notify(FlagChange('flag-3'))

    assert len(spy.statuses) == 2
    assert spy.statuses[0].key == 'flag-1'
    assert spy.statuses[1].key == 'flag-2'


def test_flag_change_listener_notified_when_value_changes():
    responses = ['initial', 'second', 'second', 'final']

    def eval_fn(key, context):
        return responses.pop(0)

    listeners = Listeners()
    tracker = FlagTrackerImpl(listeners, eval_fn)

    spy = SpyListener()
    tracker.add_flag_value_change_listener('flag-key', None, spy)
    assert len(spy.statuses) == 0

    listeners.notify(FlagChange('flag-key'))
    assert len(spy.statuses) == 1

    # No change was returned here (:second -> :second), so expect no change
    listeners.notify(FlagChange('flag-key'))
    assert len(spy.statuses) == 1

    listeners.notify(FlagChange('flag-key'))
    assert len(spy.statuses) == 2

    assert spy.statuses[0].key == 'flag-key'
    assert spy.statuses[0].old_value == 'initial'
    assert spy.statuses[0].new_value == 'second'

    assert spy.statuses[1].key == 'flag-key'
    assert spy.statuses[1].old_value == 'second'
    assert spy.statuses[1].new_value == 'final'


def test_flag_change_listener_returns_listener_we_can_unregister():
    responses = ['first', 'second', 'third']

    def eval_fn(key, context):
        return responses.pop(0)

    listeners = Listeners()
    tracker = FlagTrackerImpl(listeners, eval_fn)

    spy = SpyListener()
    created_listener = tracker.add_flag_value_change_listener('flag-key', None, spy)
    assert len(spy.statuses) == 0

    listeners.notify(FlagChange('flag-key'))
    assert len(spy.statuses) == 1

    tracker.remove_listener(created_listener)
    listeners.notify(FlagChange('flag-key'))
    assert len(spy.statuses) == 1

    assert spy.statuses[0].key == 'flag-key'
    assert spy.statuses[0].old_value == 'first'
    assert spy.statuses[0].new_value == 'second'
