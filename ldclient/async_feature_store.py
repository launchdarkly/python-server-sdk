from collections import defaultdict
from typing import Mapping, Any

from ldclient.interfaces import AsyncFeatureStore, DiagnosticDescription
from ldclient.versioned_data_kind import VersionedDataKind
from ldclient.impl.util import log


class AsyncInMemoryFeatureStore(AsyncFeatureStore, DiagnosticDescription):
    def __init__(self):
        self._initialized = False
        self._items = defaultdict(dict)

    async def get(self, kind: VersionedDataKind, key: str) -> Any:
        items_of_kind = self._items[kind]
        item = items_of_kind.get(key)
        if item is None:
            log.debug("Attempted to get missing key %s in '%s', returning None", key, kind.namespace)
            return None
        if 'deleted' in item and item['deleted']:
            log.debug("Attempted to get deleted key %s in '%s', returning None", key, kind.namespace)
            return None
        return item

    async def all(self, kind: VersionedDataKind) -> Any:
        items_of_kind = self._items[kind]
        return dict((k, i) for k, i in items_of_kind.items() if ('deleted' not in i) or not i['deleted'])

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        all_decoded = {}
        for kind, items in all_data.items():
            items_decoded = {}
            for key, item in items.items():
                items_decoded[key] = kind.decode(item)
            all_decoded[kind] = items_decoded

        self._items.clear()
        self._items.update(all_decoded)
        self._initialized = True
        for k in all_data:
            log.debug("Initialized '%s' store with %d items", k.namespace, len(all_data[k]))

    async def delete(self, kind: VersionedDataKind, key: str, version: int):
        items_of_kind = self._items[kind]
        i = items_of_kind.get(key)
        if i is None or i['version'] < version:
            i = {'deleted': True, 'version': version}
            items_of_kind[key] = i

    async def upsert(self, kind: VersionedDataKind, item: dict):
        decoded_item = kind.decode(item)
        key = item['key']

        items_of_kind = self._items[kind]
        i = items_of_kind.get(key)
        if i is None or i['version'] < item['version']:
            items_of_kind[key] = decoded_item
            log.debug("Updated %s in '%s' to version %d", key, kind.namespace, item['version'])

    @property
    def initialized(self) -> bool:
        return self._initialized

    def describe_configuration(self, config) -> str:
        return 'memory'
