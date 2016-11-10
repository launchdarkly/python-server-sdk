from ldclient.event_consumer import EventConsumerImpl
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.util import log

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FEATURES_PATH = '/flags'


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
        self.sdk_key = sdk_key
        if defaults is None:
            defaults = {}

        self.base_uri = base_uri.rstrip('\\')
        self.get_latest_features_uri = self.base_uri + GET_LATEST_FEATURES_PATH
        self.events_uri = events_uri.rstrip('\\') + '/bulk'
        self.stream_uri = stream_uri.rstrip('\\') + STREAM_FEATURES_PATH
        self.update_processor_class = update_processor_class
        self.stream = stream
        if poll_interval < 1:
            poll_interval = 1
        self.poll_interval = poll_interval
        self.use_ldd = use_ldd
        self.feature_store = InMemoryFeatureStore() if not feature_store else feature_store
        self.event_consumer_class = EventConsumerImpl if not event_consumer_class else event_consumer_class
        self.feature_requester_class = feature_requester_class
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.events_enabled = events_enabled
        self.events_upload_max_batch_size = events_upload_max_batch_size
        self.events_max_pending = events_max_pending
        self.verify_ssl = verify_ssl
        self.defaults = defaults
        self.offline = offline

    def get_default(self, key, default):
        return default if key not in self.defaults else self.defaults[key]

    @classmethod
    def default(cls):
        return cls()

    @property
    def sdk_key(self):
        return self._sdk_key

    @sdk_key.setter
    def sdk_key(self, value):
        if value is None or value is '':
            log.warn("Missing or blank sdk_key")
        self._sdk_key = value