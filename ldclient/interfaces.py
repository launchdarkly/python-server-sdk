from abc import ABCMeta, abstractmethod, abstractproperty


class FeatureStore(object):
    """
    Stores and retrieves the state of feature flags
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key, callback):
        """
        Gets a feature and calls the callback with the feature data to return the result
        :param key: The feature key
        :type key: str
        :param callback: The function that accepts the feature data and returns the feature value
        :type callback: Function that processes the feature flag once received.
        :return: The result of executing callback.
        """

    @abstractmethod
    def all(self, callback):
        """
        Returns all feature flags and their data
        :param callback: The function that accepts the feature data and returns the feature value
        :type callback: Function that processes the feature flags once received.
        :rtype: The result of executing callback.
        """

    @abstractmethod
    def init(self, features):
        """
        Initializes the store with a set of feature flags.  Meant to be called by the UpdateProcessor

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
