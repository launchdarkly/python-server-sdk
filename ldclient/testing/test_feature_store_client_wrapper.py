from threading import Event
from typing import Callable, List
from unittest.mock import Mock

from ldclient.client import _FeatureStoreClientWrapper
from ldclient.impl.datastore.status import DataStoreUpdateSinkImpl
from ldclient.impl.listeners import Listeners


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
