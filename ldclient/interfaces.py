from abc import ABCMeta, abstractmethod, abstractproperty


class FeatureStore(object):
    """
    Stores and retrieves the state of feature flags
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key):
        """
        Gets the data for a feature flag for evaluation

        :param key: The feature flag key
        :type key: str
        :return: The feature flag data
        :rtype: dict
        """

    @abstractmethod
    def all(self):
        """
        Returns all feature flags and their data

        :rtype: dict[str, dict]
        """

    @abstractmethod
    def init(self, features):
        """
        Initializes the store with a set of feature flags.  Meant to be called by the optional StreamProcessor

        :param features: The features and their data as provided by LD
        :type features: dict[str, dict]
        """

    @abstractmethod
    def delete(self, key, version):
        """
        Marks a feature flag as deleted

        :param key: The feature flag key
        :type key: str
        :param version: The version of the flag to mark as deleted
        :type version: str
        """

    @abstractmethod
    def upsert(self, key, feature):
        """
        Inserts a feature flag if its version is newer or missing

        :param key: The feature flag
        :type key: str
        :param feature: The feature information
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


class StreamProcessor(BackgroundOperation):
    """
    Populates a store from an external data source
    """
    __metaclass__ = ABCMeta


class EventConsumer(BackgroundOperation):
    """
    Consumes events from the client and sends them to LaunchDarkly
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def flush(self):
        """
        Flushes any outstanding events immediately.
        """


class FeatureRequester(object):
    """
    Requests features if they aren't in the store
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key, callback):
        """
        Gets a feature and calls the callback with the feature data to return the result

        :param key: The feature key
        :type key: str
        :param callback: The function that accepts the feature data and returns the feature value
        :type callback: function
        :return: The feature value. None if not found
        """