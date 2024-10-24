import time
from typing import Callable, Mapping, Optional, Set

from ldclient.impl.dependency_tracker import DependencyTracker, KindAndKey
from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.interfaces import (DataSourceErrorInfo, DataSourceErrorKind,
                                 DataSourceState, DataSourceStatus,
                                 DataSourceStatusProvider,
                                 DataSourceUpdateSink, FeatureStore,
                                 FlagChange)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class DataSourceUpdateSinkImpl(DataSourceUpdateSink):
    def __init__(self, store: FeatureStore, status_listeners: Listeners, flag_change_listeners: Listeners):
        self.__store = store
        self.__status_listeners = status_listeners
        self.__flag_change_listeners = flag_change_listeners
        self.__tracker = DependencyTracker()

        self.__lock = ReadWriteLock()
        self.__status = DataSourceStatus(DataSourceState.INITIALIZING, time.time(), None)

    @property
    def status(self) -> DataSourceStatus:
        try:
            self.__lock.rlock()
            return self.__status
        finally:
            self.__lock.runlock()

    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        old_data = None

        def init_store():
            nonlocal old_data
            if self.__flag_change_listeners.has_listeners():
                old_data = {}
                for kind in [FEATURES, SEGMENTS]:
                    old_data[kind] = self.__store.all(kind, lambda x: x)

            self.__store.init(all_data)

        self.__monitor_store_update(init_store)
        self.__reset_tracker_with_new_data(all_data)

        if old_data is None:
            return

        self.__send_change_events(self.__compute_changed_items_for_full_data_set(old_data, all_data))

    def upsert(self, kind: VersionedDataKind, item: dict):
        self.__monitor_store_update(lambda: self.__store.upsert(kind, item))

        # TODO(sc-212471): We only want to do this if the store successfully
        # updates the record.
        key = item.get('key', '')
        self.__update_dependency_for_single_item(kind, key, item)

    def delete(self, kind: VersionedDataKind, key: str, version: int):
        self.__monitor_store_update(lambda: self.__store.delete(kind, key, version))
        self.__update_dependency_for_single_item(kind, key, None)

    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]):
        status_to_broadcast = None

        try:
            self.__lock.lock()
            old_status = self.__status

            if new_state == DataSourceState.INTERRUPTED and old_status.state == DataSourceState.INITIALIZING:
                new_state = DataSourceState.INITIALIZING

            if new_state == old_status.state and new_error is None:
                return

            self.__status = DataSourceStatus(new_state, self.__status.since if new_state == self.__status.state else time.time(), self.__status.error if new_error is None else new_error)

            status_to_broadcast = self.__status
        finally:
            self.__lock.unlock()

        if status_to_broadcast is not None:
            self.__status_listeners.notify(status_to_broadcast)

    def __monitor_store_update(self, fn: Callable[[], None]):
        try:
            fn()
        except Exception as e:
            error_info = DataSourceErrorInfo(DataSourceErrorKind.STORE_ERROR, 0, time.time(), str(e))
            self.update_status(DataSourceState.INTERRUPTED, error_info)
            raise

    def __update_dependency_for_single_item(self, kind: VersionedDataKind, key: str, item: Optional[dict]):
        self.__tracker.update_dependencies_from(kind, key, item)
        if self.__flag_change_listeners.has_listeners():
            affected_items: Set[KindAndKey] = set()
            self.__tracker.add_affected_items(affected_items, KindAndKey(kind=kind, key=key))
            self.__send_change_events(affected_items)

    def __reset_tracker_with_new_data(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        self.__tracker.reset()

        for kind, items in all_data.items():
            for key, item in items.items():
                self.__tracker.update_dependencies_from(kind, key, item)

    def __send_change_events(self, affected_items: Set[KindAndKey]):
        for item in affected_items:
            if item.kind == FEATURES:
                self.__flag_change_listeners.notify(FlagChange(item.key))

    def __compute_changed_items_for_full_data_set(self, old_data: Mapping[VersionedDataKind, Mapping[str, dict]], new_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        affected_items: Set[KindAndKey] = set()

        for kind in [FEATURES, SEGMENTS]:
            old_items = old_data.get(kind, {})
            new_items = new_data.get(kind, {})

            keys: Set[str] = set()

            for key in keys.union(old_items.keys(), new_items.keys()):
                old_item = old_items.get(key)
                new_item = new_items.get(key)

                if old_item is None and new_item is None:
                    continue

                if old_item is None or new_item is None or old_item['version'] < new_item['version']:
                    self.__tracker.add_affected_items(affected_items, KindAndKey(kind=kind, key=key))

        return affected_items


class DataSourceStatusProviderImpl(DataSourceStatusProvider):
    def __init__(self, listeners: Listeners, update_sink: DataSourceUpdateSinkImpl):
        self.__listeners = listeners
        self.__update_sink = update_sink

    @property
    def status(self) -> DataSourceStatus:
        return self.__update_sink.status

    def add_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.add(listener)

    def remove_listener(self, listener: Callable[[DataSourceStatus], None]):
        self.__listeners.remove(listener)
