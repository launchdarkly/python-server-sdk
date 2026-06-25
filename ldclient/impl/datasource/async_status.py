import time
from typing import Dict, Mapping, Optional, Set

from ldclient.impl.dependency_tracker import DependencyTracker, KindAndKey
from ldclient.impl.listeners import Listeners
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.interfaces import (
    AsyncDataSourceUpdateSink,
    AsyncFeatureStore,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatus,
    FlagChange
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class AsyncDataSourceUpdateSinkImpl(AsyncDataSourceUpdateSink):
    def __init__(self, store: AsyncFeatureStore, status_listeners: Listeners, flag_change_listeners: Listeners):
        self.__store = store
        self.__status_listeners = status_listeners
        self.__flag_change_listeners = flag_change_listeners
        self.__tracker = DependencyTracker()

        self.__lock = ReadWriteLock()
        self.__status = DataSourceStatus(DataSourceState.INITIALIZING, time.time(), None)

    @property
    def status(self) -> DataSourceStatus:
        with self.__lock.read():
            return self.__status

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        old_data: Optional[Dict[VersionedDataKind, Mapping[str, dict]]] = None

        if self.__flag_change_listeners.has_listeners():
            old_data = {}
            for kind in [FEATURES, SEGMENTS]:
                old_data[kind] = await self.__store.all(kind)

        try:
            await self.__store.init(all_data)
        except Exception as e:
            error_info = DataSourceErrorInfo(DataSourceErrorKind.STORE_ERROR, 0, time.time(), str(e))
            self.update_status(DataSourceState.INTERRUPTED, error_info)
            raise

        self.__reset_tracker_with_new_data(all_data)

        if old_data is None:
            return

        self.__send_change_events(self.__compute_changed_items_for_full_data_set(old_data, all_data))

    async def upsert(self, kind: VersionedDataKind, item: dict) -> None:
        key = item.get('key', '')

        try:
            updated = await self.__store.upsert(kind, item)
        except Exception as e:
            error_info = DataSourceErrorInfo(DataSourceErrorKind.STORE_ERROR, 0, time.time(), str(e))
            self.update_status(DataSourceState.INTERRUPTED, error_info)
            raise

        # Only update dependency tracking and notify listeners if the store actually applied the
        # update. The AsyncFeatureStore contract returns whether it wrote, so a stale
        # (version-rejected) upsert produces no spurious flag-change events.
        if updated:
            self.__update_dependency_for_single_item(kind, key, item)

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> None:
        try:
            await self.__store.delete(kind, key, version)
        except Exception as e:
            error_info = DataSourceErrorInfo(DataSourceErrorKind.STORE_ERROR, 0, time.time(), str(e))
            self.update_status(DataSourceState.INTERRUPTED, error_info)
            raise

        self.__update_dependency_for_single_item(kind, key, None)

    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]) -> None:
        status_to_broadcast = None

        with self.__lock.write():
            old_status = self.__status

            if new_state == DataSourceState.INTERRUPTED and old_status.state == DataSourceState.INITIALIZING:
                new_state = DataSourceState.INITIALIZING

            if new_state == old_status.state and new_error is None:
                return

            self.__status = DataSourceStatus(
                new_state,
                self.__status.since if new_state == self.__status.state else time.time(),
                self.__status.error if new_error is None else new_error,
            )

            status_to_broadcast = self.__status

        if status_to_broadcast is not None:
            self.__status_listeners.notify(status_to_broadcast)

    def __update_dependency_for_single_item(self, kind: VersionedDataKind, key: str, item):
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

    def __compute_changed_items_for_full_data_set(
        self,
        old_data: Mapping[VersionedDataKind, Mapping[str, dict]],
        new_data: Mapping[VersionedDataKind, Mapping[str, dict]],
    ) -> Set[KindAndKey]:
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
