"""
This submodule contains the :class:`Config` class for custom configuration of the SDK client.

Note that the same class can also be imported from the ``ldclient.client`` submodule.
"""

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.util import log

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FLAGS_PATH = '/flags'


class Config(object):
    """Advanced configuration options for the SDK client.

    To use these options, create an instance of ``Config`` and pass it to either :func:`ldclient.set_config()`
    if you are using the singleton client, or the :class:`ldclient.client.LDClient` constructor otherwise.
    """
    def __init__(self,
                 sdk_key=None,
                 base_uri='https://app.launchdarkly.com',
                 events_uri='https://events.launchdarkly.com',
                 connect_timeout=10,
                 read_timeout=15,
                 events_max_pending=10000,
                 flush_interval=5,
                 stream_uri='https://stream.launchdarkly.com',
                 stream=True,
                 verify_ssl=True,
                 defaults=None,
                 send_events=None,
                 events_enabled=True,
                 update_processor_class=None,
                 poll_interval=30,
                 use_ldd=False,
                 feature_store=None,
                 feature_requester_class=None,
                 event_processor_class=None,
                 private_attribute_names=(),
                 all_attributes_private=False,
                 offline=False,
                 user_keys_capacity=1000,
                 user_keys_flush_interval=300,
                 inline_users_in_events=False,
                 http_proxy=None,
                 diagnostic_opt_out=False,
                 diagnostic_recording_interval=900,
                 wrapper_name=None,
                 wrapper_version=None):
        """
        :param string sdk_key: The SDK key for your LaunchDarkly account.
        :param string base_uri: The base URL for the LaunchDarkly server. Most users should use the default
          value.
        :param string events_uri: The URL for the LaunchDarkly events server. Most users should use the
          default value.
        :param float connect_timeout: The connect timeout for network connections in seconds.
        :param float read_timeout: The read timeout for network connections in seconds.
        :param int events_upload_max_batch_size: The maximum number of analytics events that the client will
          send at once.
        :param int events_max_pending: The capacity of the events buffer. The client buffers up to this many
          events in memory before flushing. If the capacity is exceeded before the buffer is flushed, events
          will be discarded.
        :param float flush_interval: The number of seconds in between flushes of the events buffer. Decreasing
          the flush interval means that the event buffer is less likely to reach capacity.
        :param string stream_uri: The URL for the LaunchDarkly streaming events server. Most users should
          use the default value.
        :param bool stream: Whether or not the streaming API should be used to receive flag updates. By
          default, it is enabled. Streaming should only be disabled on the advice of LaunchDarkly support.
        :param bool send_events: Whether or not to send events back to LaunchDarkly. This differs from
          `offline` in that it affects only the sending of client-side events, not streaming or polling for
          events from the server. By default, events will be sent.
        :param bool events_enabled: Obsolete name for `send_events`.
        :param bool offline: Whether the client should be initialized in offline mode. In offline mode,
          default values are returned for all flags and no remote network requests are made. By default,
          this is false.
        :type update_processor_class: (str, ldclient.config.Config, FeatureStore) -> UpdateProcessor
        :param float poll_interval: The number of seconds between polls for flag updates if streaming is off.
        :param bool use_ldd: Whether you are using the LaunchDarkly relay proxy in daemon mode. In this
          configuration, the client will not use a streaming connection to listen for updates, but instead
          will get feature state from a Redis instance. The `stream` and `poll_interval` options will be
          ignored if this option is set to true. By default, this is false.
        :param array private_attribute_names: Marks a set of attribute names private. Any users sent to
          LaunchDarkly with this configuration active will have attributes with these names removed.
        :param bool all_attributes_private: If true, all user attributes (other than the key) will be
          private, not just the attributes specified in `private_attribute_names`.
        :param feature_store: A FeatureStore implementation
        :type feature_store: FeatureStore
        :param int user_keys_capacity: The number of user keys that the event processor can remember at any
          one time, so that duplicate user details will not be sent in analytics events.
        :param float user_keys_flush_interval: The interval in seconds at which the event processor will
          reset its set of known user keys.
        :param bool inline_users_in_events: Whether to include full user details in every analytics event.
          By default, events will only include the user key, except for one "index" event that provides the
          full details for the user.
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :type feature_requester_class: (str, ldclient.config.Config, FeatureStore) -> FeatureRequester
        :param event_processor_class: A factory for an EventProcessor implementation taking the config
        :type event_processor_class: (ldclient.config.Config) -> EventProcessor
        :param update_processor_class: A factory for an UpdateProcessor implementation taking the sdk key,
          config, and FeatureStore implementation
        :param http_proxy: Use a proxy when connecting to LaunchDarkly. This is the full URI of the
          proxy; for example: http://my-proxy.com:1234. Note that unlike the standard `http_proxy` environment
          variable, this is used regardless of whether the target URI is HTTP or HTTPS (the actual LaunchDarkly
          service uses HTTPS, but a Relay Proxy instance could use HTTP). Setting this Config parameter will
          override any proxy specified by an environment variable, but only for LaunchDarkly SDK connections.
        :param bool diagnostic_opt_out: Unless this field is set to True, the client will send
          some diagnostics data to the LaunchDarkly servers in order to assist in the development of future SDK
          improvements. These diagnostics consist of an initial payload containing some details of SDK in use,
          the SDK's configuration, and the platform the SDK is being run on, as well as periodic information
          on irregular occurrences such as dropped events.
        :param int diagnostic_recording_interval: The interval in seconds at which periodic diagnostic data is
          sent. The default is 900 seconds (every 15 minutes) and the minimum value is 60 seconds.
        :param string wrapper_name: For use by wrapper libraries to set an identifying name for the wrapper
          being used. This will be sent in HTTP headers during requests to the LaunchDarkly servers to allow
          recording metrics on the usage of these wrapper libraries.
        :param string wrapper_version: For use by wrapper libraries to report the version of the library in
          use. If `wrapper_name` is not set, this field will be ignored. Otherwise the version string will
          be included in the HTTP headers along with the `wrapper_name` during requests to the LaunchDarkly
          servers.
        """
        self.__sdk_key = sdk_key

        if defaults is None:
            defaults = {}

        self.__base_uri = base_uri.rstrip('\\')
        self.__events_uri = events_uri.rstrip('\\')
        self.__stream_uri = stream_uri.rstrip('\\')
        self.__update_processor_class = update_processor_class
        self.__stream = stream
        self.__poll_interval = max(poll_interval, 30)
        self.__use_ldd = use_ldd
        self.__feature_store = InMemoryFeatureStore() if not feature_store else feature_store
        self.__event_processor_class = event_processor_class
        self.__feature_requester_class = feature_requester_class
        self.__connect_timeout = connect_timeout
        self.__read_timeout = read_timeout
        self.__events_max_pending = events_max_pending
        self.__flush_interval = flush_interval
        self.__verify_ssl = verify_ssl
        self.__defaults = defaults
        if offline is True:
            send_events = False
        self.__send_events = events_enabled if send_events is None else send_events
        self.__private_attribute_names = private_attribute_names
        self.__all_attributes_private = all_attributes_private
        self.__offline = offline
        self.__user_keys_capacity = user_keys_capacity
        self.__user_keys_flush_interval = user_keys_flush_interval
        self.__inline_users_in_events = inline_users_in_events
        self.__http_proxy = http_proxy
        self.__diagnostic_opt_out = diagnostic_opt_out
        self.__diagnostic_recording_interval = max(diagnostic_recording_interval, 60)
        self.__wrapper_name = wrapper_name
        self.__wrapper_version = wrapper_version

    @classmethod
    def default(cls):
        """Returns a ``Config`` instance with default values for all properties.

        :rtype: ldclient.config.Config
        """
        return cls()

    def copy_with_new_sdk_key(self, new_sdk_key):
        """Returns a new ``Config`` instance that is the same as this one, except for having a different SDK key.

        :param string new_sdk_key: the new SDK key
        :rtype: ldclient.config.Config
        """
        return Config(sdk_key=new_sdk_key,
                      base_uri=self.__base_uri,
                      events_uri=self.__events_uri,
                      connect_timeout=self.__connect_timeout,
                      read_timeout=self.__read_timeout,
                      events_max_pending=self.__events_max_pending,
                      flush_interval=self.__flush_interval,
                      stream_uri=self.__stream_uri,
                      stream=self.__stream,
                      verify_ssl=self.__verify_ssl,
                      defaults=self.__defaults,
                      send_events=self.__send_events,
                      update_processor_class=self.__update_processor_class,
                      poll_interval=self.__poll_interval,
                      use_ldd=self.__use_ldd,
                      feature_store=self.__feature_store,
                      feature_requester_class=self.__feature_requester_class,
                      event_processor_class=self.__event_processor_class,
                      private_attribute_names=self.__private_attribute_names,
                      all_attributes_private=self.__all_attributes_private,
                      offline=self.__offline,
                      user_keys_capacity=self.__user_keys_capacity,
                      user_keys_flush_interval=self.__user_keys_flush_interval,
                      inline_users_in_events=self.__inline_users_in_events,
                      diagnostic_opt_out=self.__diagnostic_opt_out,
                      diagnostic_recording_interval=self.__diagnostic_recording_interval,
                      wrapper_name=self.__wrapper_name,
                      wrapper_version=self.__wrapper_version)

    # for internal use only - probably should be part of the client logic
    def get_default(self, key, default):
        return default if key not in self.__defaults else self.__defaults[key]

    @property
    def sdk_key(self):
        return self.__sdk_key

    @property
    def base_uri(self):
        return self.__base_uri

    # for internal use only - also no longer used, will remove
    @property
    def get_latest_flags_uri(self):
        return self.__base_uri + GET_LATEST_FEATURES_PATH

    # for internal use only
    @property
    def events_base_uri(self):
        return self.__events_uri

    # for internal use only - should construct the URL path in the events code, not here
    @property
    def events_uri(self):
        return self.__events_uri + '/bulk'

    # for internal use only
    @property
    def stream_base_uri(self):
        return self.__stream_uri

    # for internal use only - should construct the URL path in the streaming code, not here
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
    def event_processor_class(self):
        return self.__event_processor_class

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
        return self.__send_events

    @property
    def send_events(self):
        return self.__send_events

    @property
    def events_max_pending(self):
        return self.__events_max_pending

    @property
    def flush_interval(self):
        return self.__flush_interval

    @property
    def verify_ssl(self):
        return self.__verify_ssl

    @property
    def private_attribute_names(self):
        return list(self.__private_attribute_names)

    @property
    def all_attributes_private(self):
        return self.__all_attributes_private

    @property
    def offline(self):
        return self.__offline

    @property
    def user_keys_capacity(self):
        return self.__user_keys_capacity

    @property
    def user_keys_flush_interval(self):
        return self.__user_keys_flush_interval

    @property
    def inline_users_in_events(self):
        return self.__inline_users_in_events

    @property
    def http_proxy(self):
        return self.__http_proxy

    @property
    def diagnostic_opt_out(self):
        return self.__diagnostic_opt_out

    @property
    def diagnostic_recording_interval(self):
        return self.__diagnostic_recording_interval

    @property
    def wrapper_name(self):
        return self.__wrapper_name

    @property
    def wrapper_version(self):
        return self.__wrapper_version

    def _validate(self):
        if self.offline is False and self.sdk_key is None or self.sdk_key == '':
            log.warning("Missing or blank sdk_key.")
