from threading import Event, Thread
from typing import Callable, List
from unittest.mock import Mock

from ldclient.impl.datastore.status import DataStoreUpdateSinkImpl
from ldclient.impl.datasystem.fdv1 import _FeatureStoreClientWrapper
from ldclient.impl.listeners import Listeners
from ldclient.versioned_data_kind import FEATURES


class CallbackListener:
    def __init__(self, fn: Callable):
        self.__fn = fn

    def __call__(self, status):
        self.__fn(status)


class RecordStatusListener:
    def __init__(self):
        self.__status = []

    def __call__(self, status):
        self.__status.append(status)

    @property
    def statuses(self) -> List:
        return self.__status


def raise_an_error():
    raise Exception('init error')


def test_store_will_not_notify_if_wrapped_store_does_not_support_monitoring():
    store = Mock()
    store.is_monitoring_enabled = lambda: False
    store.init = raise_an_error

    listener = RecordStatusListener()
    listeners = Listeners()
    listeners.add(listener)
    sink = DataStoreUpdateSinkImpl(listeners)

    wrapper = _FeatureStoreClientWrapper(store, sink)
    try:
        wrapper.init({})
        raise Exception("init should have raised an exception")
    except BaseException:
        pass

    assert len(listener.statuses) == 0


def test_store_will_not_notify_if_wrapped_store_cannot_come_back_online():
    store = Mock()
    store.is_monitoring_enabled = lambda: True
    store.init = raise_an_error

    listener = RecordStatusListener()
    listeners = Listeners()
    listeners.add(listener)
    sink = DataStoreUpdateSinkImpl(listeners)

    wrapper = _FeatureStoreClientWrapper(store, sink)
    try:
        wrapper.init({})
        raise Exception("init should have raised an exception")
    except BaseException:
        pass

    assert len(listener.statuses) == 1


def test_sink_will_be_notified_when_store_is_back_online():
    event = Event()
    statuses = []

    def set_event(status):
        statuses.append(status)
        if status.available:
            event.set()

    results = [False, True]
    store = Mock()
    store.is_monitoring_enabled = lambda: True
    store.is_available = lambda: results.pop(0)
    store.init = raise_an_error

    listener = CallbackListener(set_event)
    listeners = Listeners()
    listeners.add(listener)
    sink = DataStoreUpdateSinkImpl(listeners)

    wrapper = _FeatureStoreClientWrapper(store, sink)
    try:
        wrapper.init({})
        raise Exception("init should have raised an exception")
    except BaseException:
        pass

    event.wait(2)

    assert len(statuses) == 2
    assert statuses[0].available is False
    assert statuses[1].available is True


def test_writes_after_close_do_not_reach_the_store():
    store = Mock()
    store.is_monitoring_enabled = lambda: True

    listener = RecordStatusListener()
    listeners = Listeners()
    listeners.add(listener)
    sink = DataStoreUpdateSinkImpl(listeners)

    wrapper = _FeatureStoreClientWrapper(store, sink)
    wrapper.close()

    wrapper.init({})
    assert wrapper.upsert(FEATURES, {'key': 'flag', 'version': 1}) is False
    assert wrapper.delete(FEATURES, 'flag', 2) is False

    store.init.assert_not_called()
    store.upsert.assert_not_called()
    store.delete.assert_not_called()
    store.close.assert_called_once()
    assert len(listener.statuses) == 0


def test_close_waits_for_a_write_in_progress():
    entered = Event()
    release = Event()
    calls: List[str] = []

    def slow_upsert(kind, item):
        entered.set()
        release.wait(5)
        calls.append('upsert')
        return True

    store = Mock()
    store.is_monitoring_enabled = lambda: True
    store.upsert = slow_upsert
    store.close = lambda: calls.append('close')

    wrapper = _FeatureStoreClientWrapper(store, DataStoreUpdateSinkImpl(Listeners()))

    writer = Thread(target=lambda: wrapper.upsert(FEATURES, {'key': 'flag', 'version': 1}))
    writer.start()
    assert entered.wait(2)

    closer = Thread(target=wrapper.close)
    closer.start()
    closer.join(0.2)
    assert closer.is_alive()

    release.set()
    closer.join(2)
    writer.join(2)
    assert not closer.is_alive()
    assert calls == ['upsert', 'close']


def test_a_listener_can_close_the_wrapper_after_a_failed_write():
    store = Mock()
    store.is_monitoring_enabled = lambda: True
    store.is_available = Mock(return_value=False)
    store.init = raise_an_error

    wrapper = None
    closed = Event()

    def close_on_failure(status):
        if not status.available:
            wrapper.close()
            closed.set()

    listeners = Listeners()
    listeners.add(CallbackListener(close_on_failure))
    wrapper = _FeatureStoreClientWrapper(store, DataStoreUpdateSinkImpl(listeners))

    writer = Thread(target=lambda: _ignore_errors(lambda: wrapper.init({})), daemon=True)
    writer.start()
    writer.join(2)

    assert not writer.is_alive()
    assert closed.is_set()
    store.close.assert_called_once()


def _ignore_errors(fn):
    try:
        fn()
    except Exception:
        pass
