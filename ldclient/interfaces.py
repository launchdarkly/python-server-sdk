"""
This submodule contains interfaces for various components of the SDK.

They may be useful in writing new implementations of these components, or for testing.
"""

from abc import ABCMeta, abstractmethod, abstractproperty
from .versioned_data_kind import VersionedDataKind
from typing import Mapping, Callable, Any

class FeatureStore:
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
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, kind: VersionedDataKind, key: str, callback: Callable[[Any], Any]=lambda x: x) -> Any:
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
    def all(self, kind: VersionedDataKind, callback: Callable[[Any], Any]=lambda x: x) -> Any:
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
    def delete(self, kind: VersionedDataKind, key: str, version: int):
        """
        Deletes the object associated with the specified key, if it exists and its version is less than
        the specified version. The object should be replaced in the data store by a
        placeholder with the specified version and a "deleted" property of TErue.

        :param kind: The kind of object to delete
        :param key: The key of the object to be deleted
        :param version: The version for the delete operation
        """

    @abstractmethod
    def upsert(self, kind: VersionedDataKind, item: dict):
        """
        Updates or inserts the object associated with the specified key. If an item with the same key
        already exists, it should update it only if the new item's version property is greater than
        the old one.

        :param kind: The kind of object to update
        :param item: The object to update or insert
        """

    @abstractproperty
    def initialized(self) -> bool:
        """
        Returns whether the store has been initialized yet or not
        """


class FeatureStoreCore:
    """
    Interface for a simplified subset of the functionality of :class:`FeatureStore`, to be used
    in conjunction with :class:`ldclient.feature_store_helpers.CachingStoreWrapper`. This allows
    developers of custom ``FeatureStore`` implementations to avoid repeating logic that would
    commonly be needed in any such implementation, such as caching. Instead, they can implement
    only ``FeatureStoreCore`` and then create a ``CachingStoreWrapper``.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_internal(self, kind: VersionedDataKind, key: str) -> dict:
        """
        Returns the object to which the specified key is mapped, or None if no such item exists.
        The method should not attempt to filter out any items based on their deleted property,
        nor to cache any items.

        :param kind: The kind of object to get
        :param key: The key of the object
        :return: The object to which the specified key is mapped, or None
        """

    @abstractmethod
    def get_all_internal(self, callback) -> Mapping[str, dict]:
        """
        Returns a dictionary of all associated objects of a given kind. The method should not attempt
        to filter out any items based on their deleted property, nor to cache any items.

        :param kind: The kind of objects to get
        :return: A dictionary of keys to items
        """

    @abstractmethod
    def init_internal(self, all_data: Mapping[VersionedDataKind, Mapping[str, dict]]):
        """
        Initializes (or re-initializes) the store with the specified set of objects. Any existing entries
        will be removed. Implementations can assume that this set of objects is up to date-- there is no
        need to perform individual version comparisons between the existing objects and the supplied
        data.

        :param all_data: A dictionary of data kinds to item collections
        """

    @abstractmethod
    def upsert_internal(self, kind: VersionedDataKind, item: dict) -> dict:
        """
        Updates or inserts the object associated with the specified key. If an item with the same key
        already exists, it should update it only if the new item's version property is greater than
        the old one. It should return the final state of the item, i.e. if the update succeeded then
        it returns the item that was passed in, and if the update failed due to the version check
        then it returns the item that is currently in the data store (this ensures that
        `CachingStoreWrapper` will update the cache correctly).

        :param kind: The kind of object to update
        :param item: The object to update or insert
        :return: The state of the object after the update
        """

    @abstractmethod
    def initialized_internal(self) -> bool:
        """
        Returns true if this store has been initialized. In a shared data store, it should be able to
        detect this even if initInternal was called in a different process, i.e. the test should be
        based on looking at what is in the data store. The method does not need to worry about caching
        this value; `CachingStoreWrapper` will only call it when necessary.
        """


# Internal use only. Common methods for components that perform a task in the background.
class BackgroundOperation:

    # noinspection PyMethodMayBeStatic
    def start(self):
        """
        Starts an operation in the background.  Should return immediately and not block.
        """
        pass

    def stop(self):
        """
        Stops an operation running in the background.  May return before the operation is actually stopped.
        """
        pass

    # noinspection PyMethodMayBeStatic
    def is_alive(self) -> bool:
        """
        Returns whether the operation is alive or not
        """
        return True


class UpdateProcessor(BackgroundOperation):
    """
    Interface for the component that obtains feature flag data in some way and passes it to a
    :class:`FeatureStore`. The built-in implementations of this are the client's standard streaming
    or polling behavior. For testing purposes, there is also :func:`ldclient.integrations.Files.new_data_source()`.
    """
    __metaclass__ = ABCMeta

    def initialized(self) -> bool:
        """
        Returns whether the update processor has received feature flags and has initialized its feature store.
        """


class EventProcessor:
    """
    Interface for the component that buffers analytics events and sends them to LaunchDarkly.
    The default implementation can be replaced for testing purposes.
    """
    __metaclass__ = ABCMeta

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


class FeatureRequester:
    """
    Interface for the component that acquires feature flag data in polling mode. The default
    implementation can be replaced for testing purposes.
    """
    __metaclass__ = ABCMeta

    def get_all(self):
        """
        Gets all feature flags.
        """
        pass


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
