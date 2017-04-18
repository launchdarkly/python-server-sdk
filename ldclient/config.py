from ldclient.event_consumer import EventConsumerImpl
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.util import log

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FLAGS_PATH = '/flags'


class Config(object):
    def __init__(self,
                 sdk_key=None,
                 base_uri='https://app.launchdarkly.com',
                 events_uri='https://events.launchdarkly.com',
                 connect_timeout=10,
                 read_timeout=15,
                 events_upload_max_batch_size=100,
                 events_max_pending=10000,
                 stream_uri='https://stream.launchdarkly.com',
                 stream=True,
                 verify_ssl=True,
                 defaults=None,
                 events_enabled=True,
                 update_processor_class=None,
                 poll_interval=1,
                 use_ldd=False,
                 feature_store=InMemoryFeatureStore(),
                 feature_requester_class=None,
                 event_consumer_class=None,
                 offline=False):
        """

        :param update_processor_class: A factory for an UpdateProcessor implementation taking the sdk key, config,
                                       and FeatureStore implementation
        :type update_processor_class: (str, Config, FeatureStore) -> UpdateProcessor
        :param feature_store: A FeatureStore implementation
        :type feature_store: FeatureStore
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :type feature_requester_class: (str, Config, FeatureStore) -> FeatureRequester
        :param event_consumer_class: A factory for an EventConsumer implementation taking the event queue, sdk key, and config
        :type event_consumer_class: (queue.Queue, str, Config) -> EventConsumer
        """
        self.__sdk_key = sdk_key

        if defaults is None:
            defaults = {}

        self.__base_uri = base_uri.rstrip('\\')
        self.__events_uri = events_uri.rstrip('\\')
        self.__stream_uri = stream_uri.rstrip('\\')
        self.__update_processor_class = update_processor_class
        self.__stream = stream
        if poll_interval < 1:
            poll_interval = 1
        self.__poll_interval = poll_interval
        self.__use_ldd = use_ldd
        self.__feature_store = InMemoryFeatureStore() if not feature_store else feature_store
        self.__event_consumer_class = EventConsumerImpl if not event_consumer_class else event_consumer_class
        self.__feature_requester_class = feature_requester_class
        self.__connect_timeout = connect_timeout
        self.__read_timeout = read_timeout
        self.__events_upload_max_batch_size = events_upload_max_batch_size
        self.__events_max_pending = events_max_pending
        self.__verify_ssl = verify_ssl
        self.__defaults = defaults
        if offline is True:
            events_enabled = False
        self.__events_enabled = events_enabled
        self.__offline = offline

    @classmethod
    def default(cls):
        return cls()

    def copy_with_new_sdk_key(self, new_sdk_key):
        return Config(sdk_key=new_sdk_key,
                      base_uri=self.__base_uri,
                      events_uri=self.__events_uri,
                      connect_timeout=self.__connect_timeout,
                      read_timeout=self.__read_timeout,
                      events_upload_max_batch_size=self.__events_upload_max_batch_size,
                      events_max_pending=self.__events_max_pending,
                      stream_uri=self.__stream_uri,
                      stream=self.__stream,
                      verify_ssl=self.__verify_ssl,
                      defaults=self.__defaults,
                      events_enabled=self.__events_enabled,
                      update_processor_class=self.__update_processor_class,
                      poll_interval=self.__poll_interval,
                      use_ldd=self.__use_ldd,
                      feature_store=self.__feature_store,
                      feature_requester_class=self.__feature_requester_class,
                      event_consumer_class=self.__event_consumer_class,
                      offline=self.__offline)

    def get_default(self, key, default):
        return default if key not in self.__defaults else self.__defaults[key]

    @property
    def sdk_key(self):
        return self.__sdk_key

    @property
    def get_latest_flags_uri(self):
        return self.__base_uri + GET_LATEST_FEATURES_PATH

    @property
    def events_uri(self):
        return self.__events_uri + '/bulk'

    @property
    def stream_uri(self):
        return self.__stream_uri + STREAM_FLAGS_PATH

    @property
    def update_processor_class(self):
        return self.__update_processor_class

    @property
    def stream(self):
        return self.__stream

    @property
    def poll_interval(self):
        return self.__poll_interval

    @property
    def use_ldd(self):
        return self.__use_ldd

    @property
    def feature_store(self):
        return self.__feature_store

    @property
    def event_consumer_class(self):
        return self.__event_consumer_class

    @property
    def feature_requester_class(self):
        return self.__feature_requester_class

    @property
    def connect_timeout(self):
        return self.__connect_timeout

    @property
    def read_timeout(self):
        return self.__read_timeout

    @property
    def events_enabled(self):
        return self.__events_enabled

    @property
    def events_upload_max_batch_size(self):
        return self.__events_upload_max_batch_size

    @property
    def events_max_pending(self):
        return self.__events_max_pending

    @property
    def verify_ssl(self):
        return self.__verify_ssl

    @property
    def offline(self):
        return self.__offline

    def _validate(self):
        if self.offline is False and self.sdk_key is None or self.sdk_key is '':
            log.warn("Missing or blank sdk_key.")
