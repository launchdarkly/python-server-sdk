"""
This submodule contains interfaces for various components of the SDK.

They may be useful in writing new implementations of these components, or for testing.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Protocol
)

from ldclient.context import Context
from ldclient.impl.listeners import Listeners
from ldclient.impl.util import _Result

from .versioned_data_kind import VersionedDataKind


class DataStoreMode(Enum):
    """
    DataStoreMode represents the mode of operation of a Data Store in FDV2
    mode.
    """

    READ_ONLY = 'read-only'
    """
    READ_ONLY indicates that the data store is read-only. Data will never be
    written back to the store by the SDK.
    """

    READ_WRITE = 'read-write'
    """
    READ_WRITE indicates that the data store is read-write. Data from
    initializers/synchronizers may be written to the store as necessary.
    """


class ReadOnlyStore(Protocol):
    """ReadOnlyStore is a read-only interface for a feature store."""

    @abstractmethod
    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        raise NotImplementedError

    @abstractmethod
    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        raise NotImplementedError

    @property
    @abstractmethod
    def initialized(self) -> bool:
        raise NotImplementedError


class FeatureStore(ABC):
    """
    Interface for a versioned store for feature flags and related objects received from LaunchDarkly.
    Implementations should permit concurrent access and updates.

    An "object", for ``FeatureStore``, is simply a dict of arbitrary data which must have at least
    three properties: ``key`` (its unique key), ``version`` (the version number provided by
    LaunchDarkly), and ``deleted`` (True if this is a placeholder for a deleted object).

    Delete and upsert requests are versioned: if the version number in the request is less than
    the currently stored version of the object, the request should be ignored.

    These semantics support the primary use case for the store, which synchronizes a collection
    of objects based on update messages that may be received out-of-order.
    """

    @abstractmethod
    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        """
        Retrieves the object to which the specified key is mapped, or None if the key is not found
        or the associated object has a ``deleted`` property of True. The retrieved object, if any (a
        dict) can be transformed by the specified callback.

        :param kind: The kind of object to get
        :param key: The key whose associated object is to be returned
        :param callback: A function that accepts the retrieved data and returns a transformed value
        :return: The result of executing callback
        """

    @abstractmethod
    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any] = lambda x: x) -> Any:
        """
        Retrieves a dictionary of all associated objects of a given kind. The retrieved dict of keys
        to objects can be transformed by the specified callback.

        :param kind: The kind of objects to get
        :param callback: A function that accepts the retrieved data and returns a transformed value
        """

    @abstractmethod
    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        """
        Initializes (or re-initializes) the store with the specified set of objects. Any existing entries
        will be removed. Implementations can assume that this set of objects is up to date-- there is no
        need to perform individual version comparisons between the existing objects and the supplied data.

        :param all_data: All objects to be stored
        """

    @abstractmethod
    def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        """
        Deletes the object associated with the specified key, if it exists and its version is less than
        the specified version. The object should be replaced in the data store by a
        placeholder with the specified version and a "deleted" property of True.

        Implementations must return whether they applied the delete. The SDK uses this result to
        decide whether to send flag change events.

        :param kind: The kind of object to delete
        :param key: The key of the object to be deleted
        :param version: The version for the delete operation
        :return: True if the store was updated (a newer delete placeholder was written); False if the
            delete was rejected because the stored version was equal or newer
        """

    @abstractmethod
    def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        """
        Updates or inserts the object associated with the specified key. If an item with the same key
        already exists, it should update it only if the new item's version property is greater than
        the old one.

        Implementations must return whether they applied the update. The SDK uses this result to
        decide whether to send flag change events.

        :param kind: The kind of object to update
        :param item: The object to update or insert
        :return: True if the store was updated (the item was new or a newer version); False if the
            update was rejected because the stored version was equal or newer
        """

    @property
    @abstractmethod
    def initialized(self) -> bool:
        """
        Returns whether the store has been initialized yet or not
        """

    @abstractmethod
    def is_monitoring_enabled(self) -> bool:
        """
        Returns True if this store supports status monitoring.

        This is normally True only for persistent data stores. A custom :class:`FeatureStore`
        implementation can also return True.

        If this method returns True, the store must publish a status update when it becomes
        unavailable. An unavailable store is a store where an operation failed, or where the store
        knows that operations cannot succeed. The store must then publish another status update
        when it becomes available again. The SDK uses :func:`is_available` to detect this change.

        In-memory stores must return False.

        The SDK returns the same value from :func:`DataStoreStatusProvider.is_monitoring_enabled`.

        :return: True if the store supports status monitoring
        """

    @abstractmethod
    def is_available(self) -> bool:
        """
        Tests whether the data store operates normally.

        Do not test different kinds of operations in detail. Do the smallest possible operation
        that shows that the store works. For example, make sure that the database is reachable.

        When one of the other methods of the store raises an exception, the SDK assumes that the
        store can be unavailable. If :func:`is_monitoring_enabled` returns True, the SDK then calls
        this method at intervals until it returns True.

        A store that cannot become unavailable, such as an in-memory store, must return True.

        :return: True if the underlying data store is reachable
        """

    @abstractmethod
    def close(self) -> None:
        """
        Releases the resources that the store uses, such as database connections.

        The SDK calls this method when the client closes. A later call must have no effect.
        A store that has no resources to release can do nothing.
        """


@dataclass(frozen=True)
class SerializedItemDescriptor:
    """
    A versioned item in its serialized form, as a persistent store core reads or writes it.

    The SDK serializes each item before it gives the item to a :class:`FeatureStoreCore` or an
    :class:`AsyncFeatureStoreCore`, and deserializes each item that a core returns. A core thus
    stores and returns opaque strings, and does not need to know the data model.

    A core can store the version and the deleted state separately from the serialized string, or it
    can store only the serialized string. In the second case, the core returns a version of 0 and
    ``deleted`` as False. The SDK then gets both values from the serialized string.
    """

    version: int
    """The version of the item. A core that does not store the version separately returns 0."""

    deleted: bool
    """True if the item is a placeholder for a deleted item (a "tombstone")."""

    serialized_item: str
    """The serialized item. For a deleted item, this is a placeholder that has the deleted flag."""


class FeatureStoreCore(ABC):
    """
    Interface for a simplified subset of the functionality of :class:`FeatureStore`, to be used
    in conjunction with :class:`ldclient.feature_store_helpers.CachingStoreWrapper`. This allows
    developers of custom ``FeatureStore`` implementations to avoid repeating logic that would
    commonly be needed in any such implementation, such as caching. Instead, they can implement
    only ``FeatureStoreCore`` and then create a ``CachingStoreWrapper``.

    All items that go into and come out of these methods are :class:`SerializedItemDescriptor`
    instances. The wrapper serializes and deserializes the items, encodes deleted items, and caches
    the results. A core must not cache items.
    """

    @abstractmethod
    def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[SerializedItemDescriptor]:
        """
        Returns the item to which the specified key is mapped, or None if no such item exists.
        Return a deleted item (a "tombstone") as is. Do not filter it out.

        :param kind: The kind of item to get
        :param key: The key of the item
        :return: The serialized item, or None
        """

    @abstractmethod
    def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, SerializedItemDescriptor]:
        """
        Returns all items of the specified kind, keyed by the key that each item is stored under.
        Include deleted items. Do not filter them out.

        :param kind: The kind of items to get
        :return: A dictionary of keys to serialized items
        """

    @abstractmethod
    def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, SerializedItemDescriptor]]):
        """
        Initializes (or re-initializes) the store with the specified set of items. Remove all
        existing items. The items are up to date, so do not compare versions.

        The SDK gives the kinds and the items in an order that keeps references valid. If the store
        cannot write all items in one transaction, write them in the given order.

        :param all_data: A dictionary of data kinds to dictionaries of keys to serialized items
        """

    @abstractmethod
    def upsert_internal(self, kind: VersionedDataKind, key: str, item: SerializedItemDescriptor) -> bool:
        """
        Updates or inserts the item associated with the specified key. If an item with the same key
        already exists, update it only if the version of the new item is greater than the version
        of the existing item. A deleted item has a version, and the same rule applies to it.

        To find the version of an existing item that the core stores only as a serialized string,
        use :func:`ldclient.versioned_data_kind.VersionedDataKind.deserialize`.

        :param kind: The kind of item to update
        :param key: The key of the item
        :param item: The serialized item to write
        :return: True if the core wrote the item, or False if the version check rejected it
        """

    @abstractmethod
    def initialized_internal(self) -> bool:
        """
        Returns true if this store has been initialized. In a shared data store, it should be able to
        detect this even if init_internal was called in a different process, i.e. the test should be
        based on looking at what is in the data store. The method does not need to worry about caching
        this value; ``CachingStoreWrapper`` will only call it when necessary.
        """

    @abstractmethod
    def is_available(self) -> bool:
        """
        Tests whether the data store operates normally. This is the same as
        :func:`FeatureStore.is_available`.

        :class:`ldclient.feature_store_helpers.CachingStoreWrapper` reports status monitoring as
        enabled for all cores. When an operation fails, the SDK calls this method at intervals
        until it returns True.

        :return: True if the underlying data store is reachable
        """

    @abstractmethod
    def close(self) -> None:
        """
        Releases the resources that the core uses, such as database connections.

        :class:`ldclient.feature_store_helpers.CachingStoreWrapper` calls this method when the
        wrapper closes. A later call must have no effect.
        """


class AsyncReadOnlyStore(Protocol):
    """Read-only async view of a data store used by the async client's evaluation path.

    Both async data systems expose their active store through this uniform interface:
    :class:`AsyncFeatureStore` satisfies it directly (FDv1), and FDv2 adapts its
    synchronous in-memory store to it. This is the async analog of :class:`ReadOnlyStore`.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.
    """

    @abstractmethod
    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        raise NotImplementedError

    @abstractmethod
    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        raise NotImplementedError


class AsyncFeatureStore(ABC):
    """
    Async interface for a versioned store for feature flags and related objects received from
    LaunchDarkly. Implementations should be safe for concurrent access within a single asyncio
    event loop.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    An "object", for ``AsyncFeatureStore``, is simply a dict of arbitrary data which must have
    at least three properties: ``key`` (its unique key), ``version`` (the version number provided
    by LaunchDarkly), and ``deleted`` (True if this is a placeholder for a deleted object).

    Delete and upsert requests are versioned: if the version number in the request is less than
    the currently stored version of the object, the request should be ignored.
    """

    @abstractmethod
    async def get(self, kind: VersionedDataKind, key: str) -> Optional[Any]:
        """
        Retrieves the object to which the specified key is mapped, or None if the key is not
        found or the associated object has a ``deleted`` property of True.

        :param kind: The kind of object to get
        :param key: The key whose associated object is to be returned
        :return: The object, or None
        """

    @abstractmethod
    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        """
        Retrieves a dictionary of all associated objects of a given kind, excluding deleted items.

        :param kind: The kind of objects to get
        :return: A dictionary of keys to objects
        """

    @abstractmethod
    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        """
        Initializes (or re-initializes) the store with the specified set of objects. Any existing
        entries will be removed. Implementations can assume that this set of objects is up to
        date-- there is no need to perform individual version comparisons between the existing
        objects and the supplied data.

        :param all_data: All objects to be stored
        """

    @abstractmethod
    async def upsert(self, kind: VersionedDataKind, item: dict) -> bool:
        """
        Updates or inserts the object associated with the specified key. If an item with the same
        key already exists, it should update it only if the new item's version property is greater
        than the old one.

        :param kind: The kind of object to update
        :param item: The object to update or insert
        :return: True if the store was updated (the item was new or a newer version); False if the
            update was rejected because the stored version was equal or newer
        """

    @abstractmethod
    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> bool:
        """
        Deletes the object associated with the specified key, if it exists and its version is less
        than the specified version. The object should be replaced in the data store by a
        placeholder with the specified version and a "deleted" property of True.

        :param kind: The kind of object to delete
        :param key: The key of the object to be deleted
        :param version: The version for the delete operation
        :return: True if the store was updated (a newer delete placeholder was written); False if the
            delete was rejected because the stored version was equal or newer
        """

    @property
    @abstractmethod
    def initialized(self) -> bool:
        """
        Returns the store's last observed initialized state without querying it.
        """

    @abstractmethod
    async def is_initialized(self) -> bool:
        """
        Queries whether the store has been initialized, awaiting the store if a query is required.

        A persistent store may have been populated by another process, so this can require I/O.
        Implementations should latch a positive result: once the store is initialized it stays so.
        """

    @abstractmethod
    def is_monitoring_enabled(self) -> bool:
        """
        Returns True if this store supports status monitoring.

        This has the same meaning as :func:`FeatureStore.is_monitoring_enabled`. In-memory stores
        must return False.

        :return: True if the store supports status monitoring
        """

    @abstractmethod
    async def is_available(self) -> bool:
        """
        Tests whether the data store operates normally.

        This has the same meaning as :func:`FeatureStore.is_available`. If
        :func:`is_monitoring_enabled` returns True, the SDK calls this method at intervals after an
        operation fails, until it returns True. A store that cannot become unavailable must return
        True.

        :return: True if the underlying data store is reachable
        """

    @abstractmethod
    async def close(self) -> None:
        """
        Releases the resources that the store uses, such as database connections.

        The SDK calls this method when the client closes. A later call must have no effect.
        A store that has no resources to release can do nothing.
        """


class AsyncFeatureStoreCore(ABC):
    """
    Async equivalent of :class:`FeatureStoreCore`, for use with
    :class:`ldclient.async_feature_store_helpers.AsyncCachingStoreWrapper`. It exposes a simplified
    subset of the functionality of :class:`AsyncFeatureStore`, so a database integration only has to
    implement the database-specific logic. The wrapper adds caching and serialization.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    All items that go into and come out of these methods are :class:`SerializedItemDescriptor`
    instances, with the same rules as for :class:`FeatureStoreCore`.
    """

    @abstractmethod
    async def get_internal(self, kind: VersionedDataKind, key: str) -> Optional[SerializedItemDescriptor]:
        """
        Returns the item to which the specified key is mapped, or None if no such item exists.
        Return a deleted item (a "tombstone") as is. Do not filter it out.

        :param kind: The kind of item to get
        :param key: The key of the item
        :return: The serialized item, or None
        """
        ...

    @abstractmethod
    async def get_all_internal(self, kind: VersionedDataKind) -> Mapping[str, SerializedItemDescriptor]:
        """
        Returns all items of the specified kind, keyed by the key that each item is stored under.
        Include deleted items. Do not filter them out.

        :param kind: The kind of items to get
        :return: A dictionary of keys to serialized items
        """
        ...

    @abstractmethod
    async def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, SerializedItemDescriptor]]) -> None:
        """
        Initializes (or re-initializes) the store with the specified set of items. This is the
        same as :func:`FeatureStoreCore.init_internal`.

        :param all_data: A dictionary of data kinds to dictionaries of keys to serialized items
        """
        ...

    @abstractmethod
    async def upsert_internal(self, kind: VersionedDataKind, key: str, item: SerializedItemDescriptor) -> bool:
        """
        Updates or inserts the item associated with the specified key, if its version is greater
        than the version of the existing item. This is the same as
        :func:`FeatureStoreCore.upsert_internal`.

        :param kind: The kind of item to update
        :param key: The key of the item
        :param item: The serialized item to write
        :return: True if the core wrote the item, or False if the version check rejected it
        """
        ...

    @abstractmethod
    async def initialized_internal(self) -> bool:
        """
        Returns true if this store has been initialized. In a shared data store, it should be able to
        detect this even if init_internal was called in a different process, so the test should be
        based on what is in the data store.
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """
        Tests whether the data store operates normally. This is the same as
        :func:`AsyncFeatureStore.is_available`.

        :class:`ldclient.async_feature_store_helpers.AsyncCachingStoreWrapper` reports status
        monitoring as enabled for all cores. When an operation fails, the SDK calls this method at
        intervals until it returns True.

        :return: True if the underlying data store is reachable
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """
        Releases the resources that the core uses, such as database connections.

        :class:`ldclient.async_feature_store_helpers.AsyncCachingStoreWrapper` calls this method
        when the wrapper closes. A later call must have no effect.
        """
        ...


class UpdateProcessor(ABC):
    """
    Interface for the component that obtains feature flag data in some way and passes it to a
    :class:`FeatureStore`. The built-in implementations of this are the client's standard streaming
    or polling behavior. For testing purposes, there is also :func:`ldclient.integrations.Files.new_data_source()`.
    """

    def start(self):
        """
        Starts the update processor in the background. Returns immediately and does not block.
        """
        pass

    def stop(self):
        """
        Stops the update processor. Can return before the background work stops.
        """
        pass

    def initialized(self) -> bool:  # type: ignore[empty-body]
        """
        Returns whether the update processor has received feature flags and has initialized its feature store.
        """


class AsyncUpdateProcessor(ABC):
    """
    Async interface for the component that obtains feature flag data and passes it to an
    :class:`AsyncFeatureStore`, for use with :class:`ldclient.async_client.AsyncLDClient`. It mirrors
    :class:`UpdateProcessor`, except ``stop`` is a coroutine. The built-in implementations are the
    client's standard streaming or polling behavior.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.
    """

    def start(self):
        """
        Starts the update processor in the background. Should return immediately and not block.
        """
        pass

    async def stop(self):
        """
        Stops the update processor. Awaiting the result ensures background work has stopped.
        """
        pass

    def initialized(self) -> bool:  # type: ignore[empty-body]
        """
        Returns whether the update processor has received feature flags and has initialized its feature store.
        """


class EventProcessor(ABC):
    """
    Interface for the component that buffers analytics events and sends them to LaunchDarkly.
    The default implementation can be replaced for testing purposes.
    """

    @abstractmethod
    def send_event(self, event):
        """
        Processes an event to be sent at some point.
        """

    @abstractmethod
    def flush(self):
        """
        Specifies that any buffered events should be sent as soon as possible, rather than waiting
        for the next flush interval. This method is asynchronous, so events still may not be sent
        until a later time. However, calling ``stop()`` will synchronously deliver any events that were
        not yet delivered prior to shutting down.
        """

    @abstractmethod
    def stop(self):
        """
        Shuts down the event processor after first delivering all pending events.
        """


class AsyncEventProcessor(ABC):
    """
    Async interface for the component that buffers analytics events and sends them to LaunchDarkly,
    for use with :class:`ldclient.async_client.AsyncLDClient`. It mirrors :class:`EventProcessor`,
    except ``stop`` is a coroutine. The default implementation can be replaced for testing.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.
    """

    @abstractmethod
    def send_event(self, event):
        """
        Processes an event to be sent at some point.
        """

    @abstractmethod
    def flush(self):
        """
        Specifies that any buffered events should be sent as soon as possible, rather than waiting
        for the next flush interval. This method is not awaitable and does not wait for delivery;
        use ``flush_and_wait`` to wait for delivery to complete.
        """

    @abstractmethod
    async def flush_and_wait(self, timeout: float) -> bool:
        """
        Flushes any buffered events and waits for delivery to complete, up to ``timeout`` seconds.
        Returns True if delivery completed within the timeout, or False if it timed out. Unlike
        ``stop``, this does not shut down the event processor.
        """

    @abstractmethod
    async def stop(self):
        """
        Shuts down the event processor after first delivering all pending events.
        """


class FeatureRequester(ABC):
    """
    Interface for the component that acquires feature flag data in polling mode. The default
    implementation can be replaced for testing purposes.
    """

    def get_all(self):
        """
        Gets all feature flags.
        """
        pass

    def get_all_data(self) -> Mapping[VersionedDataKind, Mapping[str, dict]]:
        """
        Fetches all feature flag and segment data.
        """
        raise NotImplementedError


class AsyncFeatureRequester(ABC):
    """
    Async interface for the component that acquires feature flag data in polling
    mode. The default implementation can be replaced for testing purposes.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.
    """

    @abstractmethod
    async def get_all_data(self) -> Mapping[VersionedDataKind, Mapping[str, dict]]:
        """
        Fetches all feature flag and segment data.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """
        Releases any resources (such as an HTTP transport) owned by the requester.
        """
        ...


class DiagnosticDescription:
    """
    Optional interface for components to describe their own configuration.
    """

    @abstractmethod
    def describe_configuration(self, config) -> str:
        """
        Used internally by the SDK to inspect the configuration.
        :param config: the full configuration, in case this component depends on properties outside itself
        :return: a string describing the type of the component, or None
        """
        pass


class BigSegmentStoreMetadata:
    """
    Values returned by :func:`BigSegmentStore.get_metadata()`.
    """

    def __init__(self, last_up_to_date: Optional[int]):
        self.__last_up_to_date = last_up_to_date
        pass

    @property
    def last_up_to_date(self) -> Optional[int]:
        """
        The Unix epoch millisecond timestamp of the last update to the ``BigSegmentStore``. It is
        None if the store has never been updated.
        """
        return self.__last_up_to_date


class BigSegmentStore:
    """
    Interface for a read-only data store that allows querying of user membership in Big Segments.

    Big Segments are a specific type of user segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments
    """

    @abstractmethod
    def get_metadata(self) -> BigSegmentStoreMetadata:
        """
        Returns information about the overall state of the store. This method will be called only
        when the SDK needs the latest state, so it should not be cached.

        :return: the store metadata
        """
        pass

    @abstractmethod
    def get_membership(self, context_hash: str) -> Optional[dict]:
        """
        Queries the store for a snapshot of the current segment state for a specific context.

        The context_hash is a base64-encoded string produced by hashing the context key as defined
        by the Big Segments specification; the store implementation does not need to know the details
        of how this is done, because it deals only with already-hashed keys, but the string can be
        assumed to only contain characters that are valid in base64.

        The return value should be either a ``dict``, or None if the context is not referenced in any big
        segments. Each key in the dictionary is a "segment reference", which is how segments are
        identified in Big Segment data. This string is not identical to the segment key-- the SDK
        will add other information. The store implementation should not be concerned with the
        format of the string. Each value in the dictionary is True if the context is explicitly included
        in the segment, False if the context is explicitly excluded from the segment-- and is not also
        explicitly included (that is, if both an include and an exclude existed in the data, the
        include would take precedence). If the context's status in a particular segment is undefined,
        there should be no key or value for that segment.

        This dictionary may be cached by the SDK, so it should not be modified after it is created.
        It is a snapshot of the segment membership state at one point in time.

        :param context_hash: the hashed context key
        :return: True/False values for Big Segments that reference this context
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Shuts down the store component and releases and resources it is using.
        """
        pass


class AsyncBigSegmentStore(ABC):
    """
    Async interface for a read-only data store that allows querying of user membership in Big
    Segments.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    Big Segments are a specific type of user segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments
    """

    @abstractmethod
    async def get_metadata(self) -> BigSegmentStoreMetadata:
        """
        Returns information about the overall state of the store. This method will be called only
        when the SDK needs the latest state, so it should not be cached.

        :return: the store metadata
        """
        pass

    @abstractmethod
    async def get_membership(self, context_hash: str) -> Optional[dict]:
        """
        Queries the store for a snapshot of the current segment state for a specific context.

        :param context_hash: the hashed context key
        :return: True/False values for Big Segments that reference this context, or None
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """
        Shuts down the store component and releases any resources it is using.
        """
        pass


class BigSegmentStoreStatus:
    """
    Information about the state of a Big Segment store, provided by :class:`BigSegmentStoreStatusProvider`.

    Big Segments are a specific type of user segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments
    """

    def __init__(self, available: bool, stale: bool):
        self.__available = available
        self.__stale = stale

    @property
    def available(self) -> bool:
        """
        True if the Big Segment store is able to respond to queries, so that the SDK can evaluate
        whether a user is in a segment or not.

        If this property is False, the store is not able to make queries (for instance, it may not have
        a valid database connection). In this case, the SDK will treat any reference to a Big Segment
        as if no users are included in that segment. Also, the :func:`ldclient.evaluation.EvaluationDetail.reason`
        associated with with any flag evaluation that references a Big Segment when the store is not
        available will have a ``bigSegmentsStatus`` of ``"STORE_ERROR"``.
        """
        return self.__available

    @property
    def stale(self) -> bool:
        """
        True if the Big Segment store is available, but has not been updated within the amount of time
        specified by {BigSegmentsConfig#stale_after}.

        This may indicate that the LaunchDarkly Relay Proxy, which populates the store, has stopped
        running or has become unable to receive fresh data from LaunchDarkly. Any feature flag
        evaluations that reference a Big Segment will be using the last known data, which may be out
        of date. Also, the :func:`ldclient.evaluation.EvaluationDetail.reason` associated with those evaluations
        will have a ``bigSegmentsStatus`` of ``"STALE"``.
        """
        return self.__stale


class BigSegmentStoreStatusProvider:
    """
    An interface for querying the status of a Big Segment store.

    The Big Segment store is the component that receives information about Big Segments, normally
    from a database populated by the LaunchDarkly Relay Proxy. Big Segments are a specific type
    of user segments. For more information, read the LaunchDarkly documentation:
    https://docs.launchdarkly.com/home/users/big-segments

    An implementation of this abstract class is returned by :func:`ldclient.client.LDClient.big_segment_store_status_provider`.
    Application code never needs to implement this interface.

    There are two ways to interact with the status. One is to simply get the current status; if its
    ``available`` property is true, then the SDK is able to evaluate user membership in Big Segments,
    and the ``stale`` property indicates whether the data might be out of date.

    The other way is to subscribe to status change notifications. Applications may wish to know if
    there is an outage in the Big Segment store, or if it has become stale (the Relay Proxy has
    stopped updating it with new data), since then flag evaluations that reference a Big Segment
    might return incorrect values. Use :func:`add_listener()` to register a callback for notifications.
    """

    @property
    @abstractmethod
    def status(self) -> BigSegmentStoreStatus:
        """
        Gets the current status of the store.

        Before the first poll completes, this performs a blocking store query and returns its
        result, so the status is accurate even if called immediately after the client starts.

        :return: the status
        """
        pass

    @abstractmethod
    def add_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        """
        Subscribes for notifications of status changes.

        The listener is a function or method that will be called with a single parameter: the
        new ``BigSegmentStoreStatus``.

        :param listener: the listener to add
        """
        pass

    @abstractmethod
    def remove_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        """
        Unsubscribes from notifications of status changes.

        :param listener: a listener that was previously added with :func:`add_listener()`; if it was not,
            this method does nothing
        """
        pass


class AsyncBigSegmentStoreStatusProvider(ABC):
    """
    Async interface for querying the status of a Big Segment store, for use with the async
    client (:class:`ldclient.async_client.AsyncLDClient`). It mirrors
    :class:`BigSegmentStoreStatusProvider`, except the current status is read with the
    coroutine :meth:`get_status` instead of a synchronous ``status`` property (a property
    cannot await).

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.

    An implementation of this abstract class is returned by
    :meth:`ldclient.async_client.AsyncLDClient.big_segment_store_status_provider`. Application
    code never needs to implement this interface.
    """

    @abstractmethod
    async def get_status(self) -> BigSegmentStoreStatus:
        """
        Gets the current status of the store.

        Before the first poll completes, this awaits a store query and returns its result, so
        the status is accurate even if called immediately after the client starts.

        :return: the status
        """
        pass

    @abstractmethod
    def add_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        """
        Subscribes for notifications of status changes. Behaves identically to
        :meth:`BigSegmentStoreStatusProvider.add_listener`.

        :param listener: the listener to add
        """
        pass

    @abstractmethod
    def remove_listener(self, listener: Callable[[BigSegmentStoreStatus], None]) -> None:
        """
        Unsubscribes from notifications of status changes.

        :param listener: a listener that was previously added with :func:`add_listener()`; if it was not,
            this method does nothing
        """
        pass


class DataSourceState(Enum):
    """
    Enumeration representing the states a data source can be in at any given time.
    """

    INITIALIZING = 'initializing'
    """
    The initial state of the data source when the SDK is being initialized.

    If it encounters an error that requires it to retry initialization, the state will remain at
    :class:`DataSourceState.INITIALIZING` until it either succeeds and becomes {VALID}, or permanently fails and
    becomes {OFF}.
    """

    VALID = 'valid'
    """
    Indicates that the data source is currently operational and has not had any problems since the
    last time it received data.

    In streaming mode, this means that there is currently an open stream connection and that at least
    one initial message has been received on the stream. In polling mode, it means that the last poll
    request succeeded.
    """

    INTERRUPTED = 'interrupted'
    """
    Indicates that the data source encountered an error that it will attempt to recover from.

    In streaming mode, this means that the stream connection failed, or had to be dropped due to some
    other error, and will be retried after a backoff delay. In polling mode, it means that the last poll
    request failed, and a new poll request will be made after the polling interval, or after a longer
    delay if the error is one that needs to be fixed.
    """

    OFF = 'off'
    """
    Indicates that the data source is permanently shut down.

    This could be because the SDK client was explicitly shut down, because its configuration could not
    be parsed, or because the data source encountered a condition it will not retry.

    No further state or data follows. A request or connection that was still in flight when the data
    source stopped is not reported, so this state is final for the lifetime of the data source. It is
    reported when the shutdown begins rather than when the last connection closes.
    """


class DataSourceErrorKind(Enum):
    """
    Enumeration representing the types of errors a data source can encounter.
    """

    UNKNOWN = 'unknown'
    """
    An unexpected error, such as an uncaught exception.
    """

    NETWORK_ERROR = 'network_error'
    """
    An I/O error such as a dropped connection.
    """

    ERROR_RESPONSE = 'error_response'
    """
    The LaunchDarkly service returned an HTTP response with an error status.
    """

    INVALID_DATA = 'invalid_data'
    """
    The SDK received malformed data from the LaunchDarkly service.
    """

    STORE_ERROR = 'store_error'
    """
    The data source itself is working, but when it tried to put an update into the data store, the data
    store failed (so the SDK may not have the latest data).

    Data source implementations do not need to report this kind of error; it will be automatically
    reported by the SDK when exceptions are detected.
    """


class DataSourceErrorInfo:
    """
    A description of an error condition that the data source encountered.
    """

    def __init__(self, kind: DataSourceErrorKind, status_code: int, time: float, message: Optional[str]):
        self.__kind = kind
        self.__status_code = status_code
        self.__time = time
        self.__message = message

    @property
    def kind(self) -> DataSourceErrorKind:
        """
        :return: The general category of the error
        """
        return self.__kind

    @property
    def status_code(self) -> int:
        """
        :return: An HTTP status or zero.
        """
        return self.__status_code

    @property
    def time(self) -> float:
        """
        :return: Unix timestamp when the error occurred
        """
        return self.__time

    @property
    def message(self) -> Optional[str]:
        """
        :return: Message an error message if applicable, or None
        """
        return self.__message


class DataSourceStatus:
    """
    Information about the data source's status and about the last status change.
    """

    def __init__(self, state: DataSourceState, state_since: float, last_error: Optional[DataSourceErrorInfo]):
        self.__state = state
        self.__state_since = state_since
        self.__last_error = last_error

    @property
    def state(self) -> DataSourceState:
        """
        :return: The basic state of the data source.
        """
        return self.__state

    @property
    def since(self) -> float:
        """
        :return: Unix timestamp of the last state transition.
        """
        return self.__state_since

    @property
    def error(self) -> Optional[DataSourceErrorInfo]:
        """
        :return: A description of the last error, or None if there are no errors since startup
        """
        return self.__last_error


class DataSourceStatusProvider(ABC):
    """
    An interface for querying the status of the SDK's data source. The data source is the component
    that receives updates to feature flag data; normally this is a streaming connection, but it
    could be polling or file data depending on your configuration.

    An implementation of this interface is returned by
    :func:`ldclient.client.LDClient.data_source_status_provider`. Application code never needs to
    implement this interface.
    """

    @property
    @abstractmethod
    def status(self) -> DataSourceStatus:
        """
        Returns the current status of the data source.

        All the built-in data source implementations are guaranteed to update this status whenever they
        successfully initialize, encounter an error, or recover after an error.

        For a custom data source implementation, it is the responsibility of the data source to push
        status updates to the SDK; if it does not do so, the status will always be reported as
        :class:`DataSourceState.INITIALIZING`.

        :return: the status
        """
        pass

    @abstractmethod
    def add_listener(self, listener: Callable[[DataSourceStatus], None]):
        """
        Subscribes for notifications of status changes.

        The listener is a function or method that will be called with a single parameter: the
        new ``DataSourceStatus``.

        :param listener: the listener to add
        """
        pass

    @abstractmethod
    def remove_listener(self, listener: Callable[[DataSourceStatus], None]):
        """
        Unsubscribes from notifications of status changes.

        :param listener: a listener that was previously added with :func:`add_listener()`; if it was not,
            this method does nothing
        """
        pass


class DataSourceUpdateSink(ABC):
    """
    Interface that a data source implementation will use to push data into
    the SDK.

    The data source interacts with this object, rather than manipulating
    the data store directly, so that the SDK can perform any other
    necessary operations that must happen when data is updated.
    """

    @abstractmethod
    def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        """
        Initializes (or re-initializes) the store with the specified set of entities. Any
        existing entries will be removed. Implementations can assume that this data set is up to
        date-- there is no need to perform individual version comparisons between the existing
        objects and the supplied features.

        If possible, the store should update the entire data set atomically. If that is not possible,
        it should iterate through the outer hash and then the inner hash using the existing iteration
        order of those hashes (the SDK will ensure that the items were inserted into the hashes in
        the correct order), storing each item, and then delete any leftover items at the very end.

        :param all_data: All objects to be stored
        """
        pass

    @abstractmethod
    def upsert(self, kind: VersionedDataKind, item: dict):
        """
        Attempt to add an entity, or update an existing entity with the same key. An update
        should only succeed if the new item's version is greater than the old one;
        otherwise, the method should do nothing.

        :param kind: The kind of object to update
        :param item: The object to update or insert
        """
        pass

    @abstractmethod
    def delete(self, kind: VersionedDataKind, key: str, version: int):
        """
        Attempt to delete an entity if it exists. Deletion should only succeed if the
        version parameter is greater than the existing entity's version; otherwise, the
        method should do nothing.

        :param kind: The kind of object to delete
        :param key: The key of the object to be deleted
        :param version: The version for the delete operation
        """
        pass

    @abstractmethod
    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]):
        """
        Informs the SDK of a change in the data source's status.

        Data source implementations should use this method if they have any
        concept of being in a valid state, a temporarily disconnected state,
        or a permanently stopped state.

        If `new_state` is different from the previous state, and/or
        `new_error` is non-null, the SDK will start returning the new status
        (adding a timestamp for the change) from :class:`DataSourceStatusProvider.status`, and
        will trigger status change events to any registered listeners.

        A special case is that if {new_state} is :class:`DataSourceState.INTERRUPTED`, but the
        previous state was :class:`DataSourceState.INITIALIZING`, the state will remain at
        :class:`DataSourceState.INITIALIZING` because :class:`DataSourceState.INTERRUPTED` is only meaningful
        after a successful startup.

        :param new_state: The updated state of the data source
        :param new_error: An optional error if the new state is an error condition
        """
        pass


class AsyncDataSourceUpdateSink(ABC):
    """
    Async interface that a data source implementation will use to push data into the SDK.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    The data source interacts with this object, rather than manipulating the data store directly,
    so that the SDK can perform any other necessary operations that must happen when data is
    updated.
    """

    @abstractmethod
    async def init(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]) -> None:
        """
        Initializes (or re-initializes) the store with the specified set of entities. Any
        existing entries will be removed.

        :param all_data: All objects to be stored
        """
        pass

    @abstractmethod
    async def upsert(self, kind: VersionedDataKind, item: dict) -> None:
        """
        Attempt to add an entity, or update an existing entity with the same key. An update
        should only succeed if the new item's version is greater than the old one;
        otherwise, the method should do nothing.

        :param kind: The kind of object to update
        :param item: The object to update or insert
        """
        pass

    @abstractmethod
    async def delete(self, kind: VersionedDataKind, key: str, version: int) -> None:
        """
        Attempt to delete an entity if it exists. Deletion should only succeed if the
        version parameter is greater than the existing entity's version; otherwise, the
        method should do nothing.

        :param kind: The kind of object to delete
        :param key: The key of the object to be deleted
        :param version: The version for the delete operation
        """
        pass

    @abstractmethod
    def update_status(self, new_state: DataSourceState, new_error: Optional[DataSourceErrorInfo]) -> None:
        """
        Informs the SDK of a change in the data source's status.

        This method is synchronous (not async) so it can be called from any context.

        :param new_state: The updated state of the data source
        :param new_error: An optional error if the new state is an error condition
        """
        pass


class FlagChange:
    """
    Change event fired when some aspect of the flag referenced by the key has changed.
    """

    def __init__(self, key: str):
        self.__key = key

    @property
    def key(self) -> str:
        """
        :return: The flag key that was modified by the store.
        """
        return self.__key


class FlagValueChange:
    """
    Change event fired when the evaluated value for the specified flag key has changed.
    """

    def __init__(self, key, old_value, new_value):
        self.__key = key
        self.__old_value = old_value
        self.__new_value = new_value

    @property
    def key(self):
        """
        :return: The flag key that was modified by the store.
        """
        return self.__key

    @property
    def old_value(self):
        """
        :return: The old evaluation result prior to the flag changing
        """
        return self.__old_value

    @property
    def new_value(self):
        """
        :return: The new evaluation result after to the flag was changed
        """
        return self.__new_value


class FlagTracker(ABC):
    """
    An interface for tracking changes in feature flag configurations.

    An implementation of this interface is returned by :class:`ldclient.client.LDClient.flag_tracker`.
    Application code never needs to implement this interface.
    """

    @abstractmethod
    def add_listener(self, listener: Callable[[FlagChange], None]):
        """
        Registers a listener to be notified of feature flag changes in general.

        The listener will be notified whenever the SDK receives any change to any feature flag's configuration,
        or to a user segment that is referenced by a feature flag. If the updated flag is used as a prerequisite
        for other flags, the SDK assumes that those flags may now behave differently and sends flag change events
        for them as well.

        Note that this does not necessarily mean the flag's value has changed for any particular evaluation
        context, only that some part of the flag configuration was changed so that it may return a
        different value than it previously returned for some context. If you want to track flag value changes,
        use :func:`add_flag_value_change_listener` instead.

        It is possible, given current design restrictions, that a listener might be notified when no change has
        occurred. This edge case will be addressed in a later version of the SDK. It is important to note this issue
        does not affect :func:`add_flag_value_change_listener` listeners.

        If using the file data source, any change in a data file will be treated as a change to every flag. Again,
        use :func:`add_flag_value_change_listener` (or just re-evaluate the flag # yourself) if you want to know whether
        this is a change that really affects a flag's value.

        Change events only work if the SDK is actually connecting to LaunchDarkly (or using the file data source).
        If the SDK is only reading flags from a database then it cannot know when there is a change, because
        flags are read on an as-needed basis.

        The listener will be called from a worker thread.

        Calling this method for an already-registered listener has no effect.

        :param listener: listener to call when flag has changed
        """
        pass

    @abstractmethod
    def remove_listener(self, listener: Callable[[FlagChange], None]):
        """
        Unregisters a listener so that it will no longer be notified of feature flag changes.

        Calling this method for a listener that was not previously registered has no effect.

        :param listener: the listener to remove
        """
        pass

    @abstractmethod
    def add_flag_value_change_listener(self, key: str, context: Context, listener: Callable[[FlagValueChange], None]):
        """
        Registers a listener to be notified of a change in a specific feature flag's value for a specific
        evaluation context.

        When you call this method, it first immediately evaluates the feature flag. It then uses
        :func:`add_listener` to start listening for feature flag configuration
        changes, and whenever the specified feature flag changes, it re-evaluates the flag for the same context.
        It then calls your listener if and only if the resulting value has changed.

        All feature flag evaluations require an instance of :class:`ldclient.context.Context`. If the feature flag you are
        tracking does not have any context targeting rules, you must still pass a dummy context such as
        :func:`ldclient.context.Context.create("for-global-flags")`. If you do not want the user to appear on your dashboard,
        use the anonymous property which can be set via the context builder.

        The returned listener represents the subscription that was created by this method
        call; to unsubscribe, pass that object (not your listener) to :func:`remove_listener`.

        :param key: The flag key to monitor
        :param context: The context to evaluate against the flag
        :param listener: The listener to trigger if the value has changed
        """
        pass


class AsyncFlagTracker(ABC):
    """
    An interface for tracking changes in feature flag configurations, for use with the async client
    (:class:`ldclient.async_client.AsyncLDClient`). It mirrors :class:`FlagTracker`, except
    :meth:`add_flag_value_change_listener` is a coroutine.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.

    An implementation of this abstract class is returned by
    :meth:`ldclient.async_client.AsyncLDClient.flag_tracker`. Application code never needs to
    implement this interface.
    """

    @abstractmethod
    def add_listener(self, listener: Callable[[FlagChange], None]) -> None:
        """
        Registers a listener to be notified of feature flag changes in general. Behaves identically
        to :meth:`FlagTracker.add_listener`.

        :param listener: listener to call when a flag has changed
        """
        pass

    @abstractmethod
    def remove_listener(self, listener: Callable[[FlagChange], None]) -> None:
        """
        Unregisters a listener so that it will no longer be notified of feature flag changes.

        :param listener: the listener to remove
        """
        pass

    @abstractmethod
    async def add_flag_value_change_listener(self, key: str, context: Context, listener: Callable[[FlagValueChange], None]) -> Callable[[FlagChange], None]:
        """
        Registers a listener to be notified when a specific flag's evaluated value changes for a
        specific context. Behaves like :meth:`FlagTracker.add_flag_value_change_listener`, but is a
        **coroutine** and must be awaited (it evaluates the flag to capture the baseline value).

        :param key: the flag key to monitor
        :param context: the context to evaluate against the flag
        :param listener: the listener to trigger if the value has changed
        :return: the subscription; pass it to :meth:`remove_listener` to unsubscribe
        """
        pass


class DataStoreStatus:
    """
    Information about the data store's status.
    """

    def __init__(self, available: bool, stale: bool):
        self._available = available
        self._stale = stale

    @property
    def available(self) -> bool:
        """
        Returns true if the SDK believes the data store is now available.

        This property is normally true. If the SDK receives an exception while
        trying to query or update the data store, then it sets this property to
        false (notifying listeners, if any) and polls the store at intervals
        until a query succeeds. Once it succeeds, it sets the property back to
        true (again notifying listeners).

        :return: if store is available
        """
        return self._available

    @property
    def stale(self) -> bool:
        """
        Returns true if the store may be out of date due to a previous
        outage, so the SDK should attempt to refresh all feature flag data
        and rewrite it to the store.

        This property is not meaningful to application code.

        :return: true if data should be rewritten
        """
        return self._stale

    def __eq__(self, other):
        """
        Ensures two instances of DataStoreStatus are the same if their properties are the same.

        :param other: The other instance to compare
        :return: True if instances are equal, False otherwise
        """
        if isinstance(other, DataStoreStatus):
            return self._available == other._available and self._stale == other._stale
        return False


class DataStoreUpdateSink(ABC):
    """
    Interface that a data store implementation can use to report information
    back to the SDK.
    """

    @abstractmethod
    def status(self) -> DataStoreStatus:
        """
        Inspect the data store's operational status.
        """
        pass

    @abstractmethod
    def update_status(self, status: DataStoreStatus):
        """
        Reports a change in the data store's operational status.

        This is what makes the status monitoring mechanisms in
        :class:`DataStoreStatusProvider` work.

        :param status: the updated status properties
        """
        pass

    @property
    @abstractmethod
    def listeners(self) -> Listeners:
        """
        Access the listeners associated with this sink instance.
        """
        pass


class DataStoreStatusProvider(ABC):
    """
    An interface for querying the status of a persistent data store.

    An implementation of this interface is returned by :func:`ldclient.client.LDClient.data_store_status_provider`.
    Application code should not implement this interface.
    """

    @property
    @abstractmethod
    def status(self) -> DataStoreStatus:
        """
        Returns the current status of the store.

        This is only meaningful for persistent stores, or any custom data store implementation that makes use of
        the status reporting mechanism provided by the SDK. For the default in-memory store, the status will always
        be reported as "available".

        :return: the latest status
        """

    @abstractmethod
    def is_monitoring_enabled(self) -> bool:
        """
        Indicates whether the current data store implementation supports status
        monitoring.

        This is normally true for all persistent data stores, and false for the
        default in-memory store. A true value means that any listeners added
        with {#add_listener} can expect to be notified if there is any error in
        storing data, and then notified again when the error condition is
        resolved. A false value means that the status is not meaningful and
        listeners should not expect to be notified.

        :return: true if status monitoring is enabled
        """

    @abstractmethod
    def add_listener(self, listener: Callable[[DataStoreStatus], None]):
        """
        Subscribes for notifications of status changes.

        Applications may wish to know if there is an outage in a persistent
        data store, since that could mean that flag evaluations are unable to
        get the flag data from the store (unless it is currently cached) and
        therefore might return default values.

        If the SDK receives an exception while trying to query or update the
        data store, then it notifies listeners that the store appears to be
        offline ({Status#available} is false) and begins polling the store at
        intervals until a query succeeds. Once it succeeds, it notifies
        listeners again with {Status#available} set to true.

        This method has no effect if the data store implementation does not
        support status tracking, such as if you are using the default in-memory
        store rather than a persistent store.

        :param listener: the listener to add
        """

    @abstractmethod
    def remove_listener(self, listener: Callable[[DataStoreStatus], None]):
        """
        Unsubscribes from notifications of status changes.

        This method has no effect if the data store implementation does not
        support status tracking, such as if you are using the default in-memory
        store rather than a persistent store.

        :param listener: the listener to remove; if no such listener was added, this does nothing
        """


class EventName(str, Enum):
    """
    EventName represents the name of an event that can be sent by the server for FDv2.
    """

    PUT_OBJECT = "put-object"
    """
    Specifies that an object should be added to the data set with upsert semantics.
    """

    DELETE_OBJECT = "delete-object"
    """
    Specifies that an object should be removed from the data set.
    """

    SERVER_INTENT = "server-intent"
    """
    Specifies the server's intent.
    """

    PAYLOAD_TRANSFERRED = "payload-transferred"
    """
    Specifies that that all data required to bring the existing data set to
    a new version has been transferred.
    """

    HEARTBEAT = "heart-beat"
    """
    Keeps the connection alive.
    """

    GOODBYE = "goodbye"
    """
    Specifies that the server is about to close the connection.
    """

    ERROR = "error"
    """
    Specifies that an error occurred while serving the connection.
    """


@dataclass(frozen=True)
class Selector:
    """
    Selector represents a particular snapshot of data.
    """

    state: str = ""
    version: int = 0

    @staticmethod
    def no_selector() -> "Selector":
        """
        Returns an empty Selector.
        """
        return Selector()

    def is_defined(self) -> bool:
        """
        Returns True if the Selector has a value.
        """
        return self != Selector.no_selector()

    def name(self) -> str:
        """
        Event method.
        """
        return EventName.PAYLOAD_TRANSFERRED

    @staticmethod
    def new_selector(state: str, version: int) -> "Selector":
        """
        Creates a new Selector from a state string and version.
        """
        return Selector(state=state, version=version)

    def to_dict(self) -> dict:
        """
        Serializes the Selector to a JSON-compatible dictionary.
        """
        return {"state": self.state, "version": self.version}

    @staticmethod
    def from_dict(data: dict) -> "Selector":
        """
        Deserializes a Selector from a JSON-compatible dictionary.
        """
        state = data.get("state")
        version = data.get("version")

        if state is None or version is None:
            raise ValueError("Missing required fields in Selector JSON.")

        return Selector(state=state, version=version)


class ChangeType(Enum):
    """
    ChangeType specifies if an object is being upserted or deleted.
    """

    PUT = "put"
    """
    Represents an object being upserted.
    """

    DELETE = "delete"
    """
    Represents an object being deleted.
    """


class ObjectKind(str, Enum):
    """
    ObjectKind represents the kind of object.
    """

    FLAG = "flag"
    SEGMENT = "segment"


@dataclass(frozen=True)
class Change:
    """
    Change represents a change to a piece of data, such as an update or deletion.
    """

    action: ChangeType
    kind: ObjectKind
    key: str
    version: int
    object: Optional[dict] = None


class IntentCode(str, Enum):
    """
    IntentCode represents the various intents that can be sent by the server.
    """

    TRANSFER_FULL = "xfer-full"
    """
    The server intends to send a full data set.
    """
    TRANSFER_CHANGES = "xfer-changes"
    """
    The server intends to send only the necessary changes to bring an existing
    data set up-to-date.
    """

    TRANSFER_NONE = "none"
    """
    The server intends to send no data (payload is up to date).
    """


@dataclass(frozen=True)
class ChangeSet:
    """
    ChangeSet represents a list of changes to be applied.
    """

    intent_code: IntentCode
    changes: List[Change]
    selector: Selector


@dataclass(frozen=True)
class Basis:
    """
    Basis represents the initial payload of data that a data source can
    provide. Initializers provide this via fetch, whereas Synchronizers provide
    it asynchronously.
    """

    change_set: ChangeSet
    persist: bool
    environment_id: Optional[str] = None
    fallback_to_fdv1: bool = False
    """
    Indicates that the LaunchDarkly server has directed the SDK to fall
    back to the FDv1 Fallback Synchronizer (via the X-LD-FD-Fallback
    response header). When True, callers must apply ``change_set`` first
    and then terminally switch to the FDv1 Fallback Synchronizer.
    """


class ChangeSetBuilder:
    """
    ChangeSetBuilder is a helper for constructing a ChangeSet.
    """

    def __init__(self):
        """
        Initializes a new ChangeSetBuilder.
        """
        self.intent = None
        self.changes = []

    @staticmethod
    def no_changes() -> "ChangeSet":
        """
        Represents an intent that the current data is up-to-date and doesn't
        require changes.
        """
        return ChangeSet(
            intent_code=IntentCode.TRANSFER_NONE, selector=Selector.no_selector(), changes=[]
        )

    @staticmethod
    def empty(selector) -> "ChangeSet":
        """
        Returns an empty ChangeSet, which is useful for initializing a client
        without data or for clearing out all existing data.
        """
        return ChangeSet(
            intent_code=IntentCode.TRANSFER_FULL, selector=selector, changes=[]
        )

    def start(self, intent: IntentCode):
        """
        Begins a new change set with a given intent.
        """
        self.intent = intent
        self.changes = []

    def expect_changes(self):
        """
        Ensures that the current ChangeSetBuilder is prepared to handle changes.

        If a data source's initial connection reflects an updated status, we
        need to keep the provided server intent. This allows subsequent changes
        to come down the line without an explicit server intent.

        However, to maintain logical consistency, we need to ensure that the intent
        is set to IntentTransferChanges.
        """
        if self.intent is None:
            raise ValueError("changeset: cannot expect changes without a server-intent")

        if self.intent != IntentCode.TRANSFER_NONE:
            return

        self.intent = IntentCode.TRANSFER_CHANGES

    def reset(self):
        """
        Clears any existing changes while preserving the current intent.
        """
        self.changes = []

    def finish(self, selector) -> ChangeSet:
        """
        Identifies a changeset with a selector and returns the completed
        changeset. Clears any existing changes while preserving the current
        intent, so the builder can be reused.
        """
        if self.intent is None:
            raise ValueError("changeset: cannot complete without a server-intent")

        changeset = ChangeSet(
            intent_code=self.intent, selector=selector, changes=self.changes
        )
        self.changes = []

        # Once a full transfer has been processed, all future changes should be
        # assumed to be changes. Flag delivery can override this behavior by
        # sending a new server intent to any connected stream.
        if self.intent == IntentCode.TRANSFER_FULL:
            self.intent = IntentCode.TRANSFER_CHANGES

        return changeset

    def add_put(self, kind, key, version, obj):
        """
        Adds a new object to the changeset.
        """
        self.changes.append(
            Change(
                action=ChangeType.PUT, kind=kind, key=key, version=version, object=obj
            )
        )

    def add_delete(self, kind, key, version):
        """
        Adds a deletion to the changeset.
        """
        self.changes.append(
            Change(action=ChangeType.DELETE, kind=kind, key=key, version=version)
        )


@dataclass(frozen=True)
class Payload:
    """
    Payload represents a payload delivered in a streaming response.
    """

    id: str
    target: int
    code: IntentCode
    reason: str

    def to_dict(self) -> dict:
        """
        Serializes the Payload to a JSON-compatible dictionary.
        """
        return {
            "id": self.id,
            "target": self.target,
            "intentCode": self.code.value,
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "Payload":
        """
        Create a Payload from a dictionary representation.
        """
        intent_code = data.get("intentCode")

        if intent_code is None or not isinstance(intent_code, str):
            raise ValueError(
                "Invalid data for Payload: 'intentCode' key is missing or not a string"
            )

        return Payload(
            id=data.get("id", ""),
            target=data.get("target", 0),
            code=IntentCode(intent_code),
            reason=data.get("reason", ""),
        )


@dataclass(frozen=True)
class ServerIntent:
    """
    ServerIntent represents the type of change associated with the payload
    (e.g., transfer full, transfer changes, etc.)
    """

    payload: Payload

    def to_dict(self) -> dict:
        """
        Serializes the ServerIntent to a JSON-compatible dictionary.
        """
        return {
            "payloads": [self.payload.to_dict()],
        }

    @staticmethod
    def from_dict(data: dict) -> "ServerIntent":
        """
        Create a ServerIntent from a dictionary representation.
        """
        if "payloads" not in data or not isinstance(data["payloads"], list):
            raise ValueError(
                "Invalid data for ServerIntent: 'payloads' key is missing or not a list"
            )
        if len(data["payloads"]) != 1:
            raise ValueError(
                "Invalid data for ServerIntent: expected exactly one payload"
            )

        payload = data["payloads"][0]
        if not isinstance(payload, dict):
            raise ValueError("Invalid payload in ServerIntent: expected a dictionary")

        return ServerIntent(payload=Payload.from_dict(payload))


class SelectorStore(Protocol):
    """
    SelectorStore represents a component capable of providing Selectors
    for data retrieval.
    """

    @abstractmethod
    def selector(self) -> Selector:
        """
        get_selector should return a Selector object that defines the criteria
        for data retrieval.
        """
        raise NotImplementedError


BasisResult = _Result[Basis, str]


class Initializer(Protocol):  # pylint: disable=too-few-public-methods
    """
    Initializer represents a component capable of retrieving a single data
    result, such as from the LD polling API.

    The intent of initializers is to quickly fetch an initial set of data,
    which may be stale but is fast to retrieve. This initial data serves as a
    foundation for a Synchronizer to build upon, enabling it to provide updates
    as new changes occur.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of the initializer, which is used for logging and debugging.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch(self, ss: SelectorStore) -> BasisResult:
        """
        fetch should retrieve the initial data set for the data source, returning
        a Basis object on success, or an error message on failure.

        :param ss: A SelectorStore that provides the Selector to use as a basis for data retrieval.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Update:
    """
    Update represents the results of a synchronizer's ongoing sync
    method.
    """

    state: DataSourceState
    change_set: Optional[ChangeSet] = None
    error: Optional[DataSourceErrorInfo] = None
    fallback_to_fdv1: bool = False
    """
    Indicates that the LaunchDarkly server has directed the SDK to fall
    back to the FDv1 Fallback Synchronizer (via the X-LD-FD-Fallback
    response header). When True, callers must apply any accompanying
    ``change_set`` first and then terminally switch to the FDv1 Fallback
    Synchronizer. The flag may ride along on Valid or Off updates.
    """
    environment_id: Optional[str] = None


class Synchronizer(Protocol):  # pylint: disable=too-few-public-methods
    """
    Synchronizer represents a component capable of synchronizing data from an external
    data source, such as a streaming or polling API.

    It is responsible for yielding Update objects that represent the current state
    of the data source, including any changes that have occurred since the last
    synchronization.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of the synchronizer, which is used for logging and debugging.
        """
        raise NotImplementedError

    @abstractmethod
    def sync(self, ss: SelectorStore) -> Generator[Update, None, None]:
        """
        sync should begin the synchronization process for the data source, yielding
        Update objects until the connection is closed or an unrecoverable error
        occurs.

        :param ss: A SelectorStore that provides the Selector to use as a basis for data retrieval.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """
        stop should halt the synchronization process, causing the sync method
        to exit as soon as possible.
        """
        raise NotImplementedError


class AsyncInitializer(Protocol):  # pylint: disable=too-few-public-methods
    """
    AsyncInitializer represents a component capable of retrieving a single data result
    asynchronously, such as from the LD polling API.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    The intent of initializers is to quickly fetch an initial set of data, which may be stale but
    is fast to retrieve. This initial data serves as a foundation for a Synchronizer to build upon,
    enabling it to provide updates as new changes occur.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of the initializer, which is used for logging and debugging.
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch(self, ss: SelectorStore) -> BasisResult:
        """
        fetch should retrieve the initial data set for the data source asynchronously, returning
        a Basis object on success, or an error message on failure.

        :param ss: A SelectorStore that provides the Selector to use as a basis for data retrieval.
        """
        raise NotImplementedError


class AsyncSynchronizer(Protocol):  # pylint: disable=too-few-public-methods
    """
    AsyncSynchronizer represents a component capable of synchronizing data from an external
    data source asynchronously, such as a streaming or polling API.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.

    It is responsible for yielding Update objects that represent the current state of the data
    source, including any changes that have occurred since the last synchronization.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of the synchronizer, which is used for logging and debugging.
        """
        raise NotImplementedError

    @abstractmethod
    def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        """
        sync should begin the synchronization process for the data source, yielding Update objects
        asynchronously until the connection is closed or an unrecoverable error occurs.

        Implementations write this as ``async def sync(): yield ...`` (an async generator) and
        callers consume it as ``async for update in impl.sync(ss):``.

        :param ss: A SelectorStore that provides the Selector to use as a basis for data retrieval.
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """
        stop should halt the synchronization process, causing the sync method to exit as soon as
        possible.
        """
        raise NotImplementedError
