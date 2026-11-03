"""Converts between plain item dicts and serialized items, for test store cores."""

import json
from typing import Mapping, Optional

from ldclient.interfaces import SerializedItemDescriptor


def to_descriptor(item: Optional[dict]) -> Optional[SerializedItemDescriptor]:
    if item is None:
        return None
    return SerializedItemDescriptor(version=item.get('version', 0), deleted=item.get('deleted') is True, serialized_item=json.dumps(item))


def to_descriptors(items: Optional[Mapping[str, dict]]) -> Optional[Mapping[str, SerializedItemDescriptor]]:
    if items is None:
        return None
    return {key: to_descriptor(item) for key, item in items.items()}  # type: ignore[misc]


def from_descriptor(descriptor: SerializedItemDescriptor) -> dict:
    return json.loads(descriptor.serialized_item)


def from_descriptors(all_data) -> dict:
    return {kind: {key: from_descriptor(d) for key, d in items.items()} for kind, items in all_data.items()}
