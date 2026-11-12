import json
from typing import Mapping, Optional

from ldclient.interfaces import SerializedItemDescriptor
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind

# The suites use two kinds, so that a test can show that the kinds do not share items.
KIND = FEATURES
OTHER_KIND = SEGMENTS


def item(key: str, version: int, name: str = '') -> SerializedItemDescriptor:
    """Returns a serialized item. The name lets two items with the same version differ."""
    data = {'key': key, 'version': version, 'name': name}
    return SerializedItemDescriptor(version=version, deleted=False, serialized_item=json.dumps(data))


def deleted_item(key: str, version: int) -> SerializedItemDescriptor:
    """Returns a serialized placeholder for a deleted item."""
    data = {'key': key, 'version': version, 'deleted': True}
    return SerializedItemDescriptor(version=version, deleted=True, serialized_item=json.dumps(data))


def keyless_deleted_item(version: int) -> SerializedItemDescriptor:
    """Returns a deleted item as other LaunchDarkly SDKs write it, with a version and no key."""
    data = {'version': version, 'deleted': True}
    return SerializedItemDescriptor(version=version, deleted=True, serialized_item=json.dumps(data))


def data_set(*items: SerializedItemDescriptor, other: Optional[Mapping[str, SerializedItemDescriptor]] = None) -> Mapping[VersionedDataKind, Mapping[str, SerializedItemDescriptor]]:
    """Returns a full data set with the given items of KIND and the other items of OTHER_KIND."""
    return {
        KIND: {_key(i): i for i in items},
        OTHER_KIND: dict(other or {}),
    }


def assert_item(expected: SerializedItemDescriptor, actual: Optional[SerializedItemDescriptor]):
    """
    Asserts that a core returned the expected item.

    A core can store the version only in the serialized item. It then returns a version of 0, and
    this check accepts that.
    """
    assert actual is not None
    assert actual.serialized_item == expected.serialized_item
    assert not actual.deleted
    if actual.version != 0:
        assert actual.version == expected.version


def assert_deleted(expected_version: int, actual: Optional[SerializedItemDescriptor]):
    """
    Asserts that a core returned a deleted item with the expected version.

    A core can store the version and the deleted state separately, or only in the serialized
    item. This check accepts both, as the caching wrapper does.
    """
    assert actual is not None
    parsed = json.loads(actual.serialized_item)
    assert actual.deleted or parsed.get('deleted') is True
    assert (actual.version or parsed.get('version')) == expected_version


def _key(descriptor: SerializedItemDescriptor) -> str:
    return json.loads(descriptor.serialized_item)['key']
