"""
This submodule contains interfaces for various components of the SDK.

They may be useful in writing new implementations of these components, or for testing.
"""

from abc import ABCMeta, abstractmethod, abstractproperty
from enum import Enum
from typing import Any, Callable, Mapping, Optional

from ldclient.context import Context
from ldclient.impl.listeners import Listeners

from .versioned_data_kind import VersionedDataKind


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

    # WARN: This isn't a required method on a FeatureStore yet. The SDK will
    # currently check if the provided store responds to this method, and if
    # it does, will take appropriate action based on the documented behavior
    # below. This will become required in a future major version release of
    # the SDK.
    #
    # @abstractmethod
    # def is_monitoring_enabled(self) -> bool:
    #     """
    #     Returns true if this data store implementation supports status
    #     monitoring.
    #
    #     This is normally only true for persistent data stores but it could also
    #     be true for any custom :class:`FeatureStore` implementation.
    #
    #     Returning true means that the store guarantees that if it ever enters
    #     an invalid state (that is, an operation has failed or it knows that
    #     operations cannot succeed at the moment), it will publish a status
    #     update, and will then publish another status update once it has
    #     returned to a valid state.
    #
    #     Custom implementations must implement :func:`FeatureStore.is_available`
    #     which synchronously checks if the store is available. Without this
    #     method, the SDK cannot ensure status updates will occur once the store
    #     has gone offline.
    #
    #     The same value will be returned from
    #     :func:`DataStoreStatusProvider.is_monitoring_enabled`.
    #     """

    # WARN: This isn't a required method on a FeatureStore. The SDK will
    # check if the provided store responds to this method, and if it does,
    # will take appropriate action based on the documented behavior below.
    # Usage of this method will be dropped in a future version of the SDK.
    #
    # @abstractmethod
    # def is_available(self) -> bool:
    #     """
    #     Tests whether the data store seems to be functioning normally.
    #
    #     This should not be a detailed test of different kinds of operations,
    #     but just the smallest possible operation to determine whether (for
    #     instance) we can reach the database.
    #
    #     Whenever one of the store's other methods throws an exception, the SDK
    #     will assume that it may have become unavailable (e.g. the database
    #     connection was lost). The SDK will then call is_available at intervals
    #     until it returns true.
    #
    #     :return: true if the underlying data store is reachable
    #     """


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
        ``CachingStoreWrapper`` will update the cache correctly).

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
        this value; ``CachingStoreWrapper`` will only call it when necessary.
        """

    # WARN: This isn't a required method on a FeatureStoreCore. The SDK will
    # check if the provided store responds to this method, and if it does,
    # will take appropriate action based on the documented behavior below.
    # Usage of this method will be dropped in a future version of the SDK.
    #
    # @abstractmethod
    # def is_available(self) -> bool:
    #     """
    #     Tests whether the data store seems to be functioning normally.
    #
    #     This should not be a detailed test of different kinds of operations,
    #     but just the smallest possible operation to determine whether (for
    #     instance) we can reach the database.
    #
    #     Whenever one of the store's other methods throws an exception, the SDK
    #     will assume that it may have become unavailable (e.g. the database
    #     connection was lost). The SDK will then call is_available at intervals
    #     until it returns true.
    #
    #     :return: true if the underlying data store is reachable
    #     """


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

    def initialized(self) -> bool:  # type: ignore[empty-body]
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

    @abstractproperty
    def status(self) -> BigSegmentStoreStatus:
        """
        Gets the current status of the store.

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
    request failed, and a new poll request will be made after the configured polling interval.
    """

    OFF = 'off'
    """
    Indicates that the data source has been permanently shut down.

    This could be because it encountered an unrecoverable error (for instance, the LaunchDarkly service
    rejected the SDK key; an invalid SDK key will never become valid), or because the SDK client was
    explicitly shut down.
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


class DataSourceStatusProvider:
    """
    An interface for querying the status of the SDK's data source. The data source is the component
    that receives updates to feature flag data; normally this is a streaming connection, but it
    could be polling or file data depending on your configuration.

    An implementation of this interface is returned by
    :func:`ldclient.client.LDClient.data_source_status_provider`. Application code never needs to
    implement this interface.
    """

    __metaclass__ = ABCMeta

    @abstractproperty
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


class DataSourceUpdateSink:
    """
    Interface that a data source implementation will use to push data into
    the SDK.

    The data source interacts with this object, rather than manipulating
    the data store directly, so that the SDK can perform any other
    necessary operations that must happen when data is updated.
    """

    __metaclass__ = ABCMeta

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


class FlagTracker:
    """
    An interface for tracking changes in feature flag configurations.

    An implementation of this interface is returned by :class:`ldclient.client.LDClient.flag_tracker`.
    Application code never needs to implement this interface.
    """

    __metaclass__ = ABCMeta

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


class DataStoreStatus:
    """
    Information about the data store's status.
    """

    __metaclass__ = ABCMeta

    def __init__(self, available: bool, stale: bool):
        self.__available = available
        self.__stale = stale

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
        return self.__available

    @property
    def stale(self) -> bool:
        """
        Returns true if the store may be out of date due to a previous
        outage, so the SDK should attempt to refresh all feature flag data
        and rewrite it to the store.

        This property is not meaningful to application code.

        :return: true if data should be rewritten
        """
        return self.__stale


class DataStoreUpdateSink:
    """
    Interface that a data store implementation can use to report information
    back to the SDK.
    """

    __metaclass__ = ABCMeta

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

    @abstractproperty
    def listeners(self) -> Listeners:
        """
        Access the listeners associated with this sink instance.
        """
        pass


class DataStoreStatusProvider:
    """
    An interface for querying the status of a persistent data store.

    An implementation of this interface is returned by :func:`ldclient.client.LDClient.data_store_status_provider`.
    Application code should not implement this interface.
    """

    __metaclass__ = ABCMeta

    @abstractproperty
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
