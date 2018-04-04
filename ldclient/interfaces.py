from abc import ABCMeta, abstractmethod, abstractproperty


class FeatureStore(object):
    """
    Stores and retrieves the state of feature flags and related data
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, kind, key, callback):
        """
        Gets a feature and calls the callback with the feature data to return the result
        :param kind: Denotes which collection to access - one of the constants in versioned_data_kind
        :type kind: VersionedDataKind
        :param key: The key of the object
        :type key: str
        :param callback: The function that accepts the retrieved data and returns a transformed value
        :type callback: Function that processes the retrieved object once received.
        :return: The result of executing callback.
        """

    @abstractmethod
    def all(self, callback):
        """
        Returns all feature flags and their data
        :param kind: Denotes which collection to access - one of the constants in versioned_data_kind
        :type kind: VersionedDataKind
        :param callback: The function that accepts the retrieved data and returns a transformed value
        :type callback: Function that processes the retrieved objects once received.
        :rtype: The result of executing callback.
        """

    @abstractmethod
    def init(self, all_data):
        """
        Initializes the store with a set of objects.  Meant to be called by the UpdateProcessor

        :param all_data: The features and their data as provided by LD
        :type all_data: dict[VersionedDataKind, dict[str, dict]]
        """

    @abstractmethod
    def delete(self, kind, key, version):
        """
        Marks an object as deleted

        :param kind: Denotes which collection to access - one of the constants in versioned_data_kind
        :type kind: VersionedDataKind
        :param key: The object key
        :type key: str
        :param version: The version of the object to mark as deleted
        :type version: int
        """

    @abstractmethod
    def upsert(self, kind, item):
        """
        Inserts an object if its version is newer or missing

        :param kind: Denotes which collection to access - one of the constants in versioned_data_kind
        :type kind: VersionedDataKind
        :param item: The object to be inserted or updated - must have key and version properties
        :type feature: dict
        """

    @abstractproperty
    def initialized(self):
        """
        Returns whether the store has been initialized yet or not

        :rtype: bool
        """


class BackgroundOperation(object):
    """
    Performs a task in the background
    """

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
    def is_alive(self):
        """
        Returns whether the operation is alive or not
        :rtype: bool
        """
        return True


class UpdateProcessor(BackgroundOperation):
    """
    Responsible for retrieving Feature Flag updates from LaunchDarkly and saving them to the feature store
    """
    __metaclass__ = ABCMeta

    def initialized(self):
        """
        Returns whether the update processor has received feature flags and has initialized its feature store.
        :rtype: bool
        """


class EventProcessor(object):
    """
    Buffers analytics events and sends them to LaunchDarkly
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
        until a later time. However, calling stop() will synchronously deliver any events that were
        not yet delivered prior to shutting down.
        """
    
    @abstractmethod
    def stop(self):
        """
        Shuts down the event processor after first delivering all pending events.
        """


class FeatureRequester(object):
    """
    Requests features.
    """
    __metaclass__ = ABCMeta

    def get_all(self):
        """
        Gets all feature flags.
        """
        pass

    def get_one(self, key):
        """
        Gets one Feature flag
        :return:
        """
        pass
