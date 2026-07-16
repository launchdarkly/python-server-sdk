"""
This submodule contains the async in-memory feature store implementation.
"""

from collections import defaultdict
from typing import Any, Dict, Mapping, Optional

from ldclient.impl.util import log
from ldclient.interfaces import AsyncFeatureStore, DiagnosticDescription
from ldclient.versioned_data_kind import VersionedDataKind


class AsyncInMemoryFeatureStore(AsyncFeatureStore, DiagnosticDescription):
    """The default async feature store implementation, which holds all data in memory.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.
    """

    def __init__(self):
        """Constructs an instance of AsyncInMemoryFeatureStore."""
        self._initialized = False
        self._items: Dict[VersionedDataKind, Dict[str, Any]] = defaultdict(dict)

    def is_monitoring_enabled(self) -> bool:
        return False

    def is_available(self) -> bool:
        return True

    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        """ """
        items_of_kind = self._items[kind]
        item = items_of_kind.get(key)
        if item is None:
            log.debug("Attempted to get missing key %s in '%s', returning None", key, kind.namespace)
            return None
        if 'deleted' in item and item['deleted']:
            log.debug("Attempted to get deleted key %s in '%s', returning None", key, kind.namespace)
            return None
        return item

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        """ """
        items_of_kind = self._items[kind]
        return dict((k, i) for k, i in items_of_kind.items() if ('deleted' not in i) or not i['deleted'])

    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        """ """
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

    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> None:
        """ """
        items_of_kind = self._items[kind]
        i = items_of_kind.get(key)
        if i is None or i['version'] < version:
            items_of_kind[key] = {'deleted': True, 'version': version}

    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        """ """
        decoded_item = kind.decode(item)
        key = item['key']
        items_of_kind = self._items[kind]
        i = items_of_kind.get(key)
        if i is None or i['version'] < item['version']:
            items_of_kind[key] = decoded_item
            log.debug("Updated %s in '%s' to version %d", key, kind.namespace, item['version'])
            return True
        return False

    @property
    def initialized(self) -> bool:
        """ """
        return self._initialized

    async def close(self) -> None:
        """ """
        pass

    def describe_configuration(self, config) -> str:
        return 'memory'
