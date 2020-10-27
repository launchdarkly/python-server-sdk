"""
This submodule contains the :class:`Config` class for custom configuration of the SDK client.

Note that the same class can also be imported from the ``ldclient.client`` submodule.
"""

from typing import Optional, Callable, List, Any, Set

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.util import log
from ldclient.interfaces import EventProcessor, FeatureStore, UpdateProcessor, FeatureRequester

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FLAGS_PATH = '/flags'


class HTTPConfig:
    """Advanced HTTP configuration options for the SDK client.

    This class groups together HTTP/HTTPS-related configuration properties that rarely need to be changed.
    If you need to set these, construct an `HTTPConfig` instance and pass it as the `http` parameter when
    you construct the main :class:`Config` for the SDK client.
    """
    def __init__(self,
                 connect_timeout: float=10,
                 read_timeout: float=15,
                 http_proxy: Optional[str]=None,
                 ca_certs: Optional[str]=None,
                 cert_file: Optional[str]=None,
                 disable_ssl_verification: bool=False):
        """
        :param connect_timeout: The connect timeout for network connections in seconds.
        :param read_timeout: The read timeout for network connections in seconds.
        :param http_proxy: Use a proxy when connecting to LaunchDarkly. This is the full URI of the
          proxy; for example: http://my-proxy.com:1234. Note that unlike the standard `http_proxy` environment
          variable, this is used regardless of whether the target URI is HTTP or HTTPS (the actual LaunchDarkly
          service uses HTTPS, but a Relay Proxy instance could use HTTP). Setting this Config parameter will
          override any proxy specified by an environment variable, but only for LaunchDarkly SDK connections.
        :param ca_certs: If using a custom certificate authority, set this to the file path of the
          certificate bundle.
        :param cert_file: If using a custom client certificate, set this to the file path of the
          certificate.
        :param disable_ssl_verification: If true, completely disables SSL verification and certificate
          verification for secure requests. This is unsafe and should not be used in a production environment;
          instead, use a self-signed certificate and set `ca_certs`.
        """
        self.__connect_timeout = connect_timeout
        self.__read_timeout = read_timeout
        self.__http_proxy = http_proxy
        self.__ca_certs = ca_certs
        self.__cert_file = cert_file
        self.__disable_ssl_verification = disable_ssl_verification

    @property
    def connect_timeout(self) -> float:
        return self.__connect_timeout

    @property
    def read_timeout(self) -> float:
        return self.__read_timeout

    @property
    def http_proxy(self) -> Optional[str]:
        return self.__http_proxy

    @property
    def ca_certs(self) -> Optional[str]:
        return self.__ca_certs

    @property
    def cert_file(self) -> Optional[str]:
        return self.__cert_file

    @property
    def disable_ssl_verification(self) -> bool:
        return self.__disable_ssl_verification

class Config:
    """Advanced configuration options for the SDK client.

    To use these options, create an instance of ``Config`` and pass it to either :func:`ldclient.set_config()`
    if you are using the singleton client, or the :class:`ldclient.client.LDClient` constructor otherwise.
    """
    def __init__(self,
                 sdk_key: str,
                 base_uri: str='https://app.launchdarkly.com',
                 events_uri: str='https://events.launchdarkly.com',
                 events_max_pending: int=10000,
                 flush_interval: float=5,
                 stream_uri: str='https://stream.launchdarkly.com',
                 stream: bool=True,
                 initial_reconnect_delay: float=1,
                 defaults: dict={},
                 send_events: Optional[bool]=None,
                 events_enabled: bool=True,
                 update_processor_class: Optional[Callable[[str, 'Config', FeatureStore], UpdateProcessor]]=None, 
                 poll_interval: float=30,
                 use_ldd: bool=False,
                 feature_store: Optional[FeatureStore]=None,
                 feature_requester_class=None,
                 event_processor_class: Callable[['Config'], EventProcessor]=None, 
                 private_attribute_names: Set[str]=set(),
                 all_attributes_private: bool=False,
                 offline: bool=False,
                 user_keys_capacity: int=1000,
                 user_keys_flush_interval: float=300,
                 inline_users_in_events: bool=False,
                 diagnostic_opt_out: bool=False,
                 diagnostic_recording_interval: int=900,
                 wrapper_name: Optional[str]=None,
                 wrapper_version: Optional[str]=None,
                 http: HTTPConfig=HTTPConfig()):
        """
        :param sdk_key: The SDK key for your LaunchDarkly account. This is always required.
        :param base_uri: The base URL for the LaunchDarkly server. Most users should use the default
          value.
        :param events_uri: The URL for the LaunchDarkly events server. Most users should use the
          default value.
        :param events_max_pending: The capacity of the events buffer. The client buffers up to this many
          events in memory before flushing. If the capacity is exceeded before the buffer is flushed, events
          will be discarded.
        :param flush_interval: The number of seconds in between flushes of the events buffer. Decreasing
          the flush interval means that the event buffer is less likely to reach capacity.
        :param stream_uri: The URL for the LaunchDarkly streaming events server. Most users should
          use the default value.
        :param stream: Whether or not the streaming API should be used to receive flag updates. By
          default, it is enabled. Streaming should only be disabled on the advice of LaunchDarkly support.
        :param initial_reconnect_delay: The initial reconnect delay (in seconds) for the streaming
          connection. The streaming service uses a backoff algorithm (with jitter) every time the connection needs
          to be reestablished. The delay for the first reconnection will start near this value, and then
          increase exponentially for any subsequent connection failures.
        :param send_events: Whether or not to send events back to LaunchDarkly. This differs from
          `offline` in that it affects only the sending of client-side events, not streaming or polling for
          events from the server. By default, events will be sent.
        :param events_enabled: Obsolete name for `send_events`.
        :param offline: Whether the client should be initialized in offline mode. In offline mode,
          default values are returned for all flags and no remote network requests are made. By default,
          this is false.
        :param poll_interval: The number of seconds between polls for flag updates if streaming is off.
        :param use_ldd: Whether you are using the LaunchDarkly relay proxy in daemon mode. In this
          configuration, the client will not use a streaming connection to listen for updates, but instead
          will get feature state from a Redis instance. The `stream` and `poll_interval` options will be
          ignored if this option is set to true. By default, this is false.
        :param array private_attribute_names: Marks a set of attribute names private. Any users sent to
          LaunchDarkly with this configuration active will have attributes with these names removed.
        :param all_attributes_private: If true, all user attributes (other than the key) will be
          private, not just the attributes specified in `private_attribute_names`.
        :param feature_store: A FeatureStore implementation
        :param user_keys_capacity: The number of user keys that the event processor can remember at any
          one time, so that duplicate user details will not be sent in analytics events.
        :param user_keys_flush_interval: The interval in seconds at which the event processor will
          reset its set of known user keys.
        :param inline_users_in_events: Whether to include full user details in every analytics event.
          By default, events will only include the user key, except for one "index" event that provides the
          full details for the user.
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :param event_processor_class: A factory for an EventProcessor implementation taking the config
        :param update_processor_class: A factory for an UpdateProcessor implementation taking the sdk key,
          config, and FeatureStore implementation
        :param diagnostic_opt_out: Unless this field is set to True, the client will send
          some diagnostics data to the LaunchDarkly servers in order to assist in the development of future SDK
          improvements. These diagnostics consist of an initial payload containing some details of SDK in use,
          the SDK's configuration, and the platform the SDK is being run on, as well as periodic information
          on irregular occurrences such as dropped events.
        :param diagnostic_recording_interval: The interval in seconds at which periodic diagnostic data is
          sent. The default is 900 seconds (every 15 minutes) and the minimum value is 60 seconds.
        :param wrapper_name: For use by wrapper libraries to set an identifying name for the wrapper
          being used. This will be sent in HTTP headers during requests to the LaunchDarkly servers to allow
          recording metrics on the usage of these wrapper libraries.
        :param wrapper_version: For use by wrapper libraries to report the version of the library in
          use. If `wrapper_name` is not set, this field will be ignored. Otherwise the version string will
          be included in the HTTP headers along with the `wrapper_name` during requests to the LaunchDarkly
          servers.
        :param http: Optional properties for customizing the client's HTTP/HTTPS behavior. See
          :class:`HTTPConfig`.
        """
        self.__sdk_key = sdk_key

        self.__base_uri = base_uri.rstrip('\\')
        self.__events_uri = events_uri.rstrip('\\')
        self.__stream_uri = stream_uri.rstrip('\\')
        self.__update_processor_class = update_processor_class
        self.__stream = stream
        self.__initial_reconnect_delay = initial_reconnect_delay
        self.__poll_interval = max(poll_interval, 30.0)
        self.__use_ldd = use_ldd
        self.__feature_store = InMemoryFeatureStore() if not feature_store else feature_store
        self.__event_processor_class = event_processor_class
        self.__feature_requester_class = feature_requester_class
        self.__events_max_pending = events_max_pending
        self.__flush_interval = flush_interval
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
        self.__diagnostic_opt_out = diagnostic_opt_out
        self.__diagnostic_recording_interval = max(diagnostic_recording_interval, 60)
        self.__wrapper_name = wrapper_name
        self.__wrapper_version = wrapper_version
        self.__http = http

    def copy_with_new_sdk_key(self, new_sdk_key: str) -> 'Config':
        """Returns a new ``Config`` instance that is the same as this one, except for having a different SDK key.

        :param new_sdk_key: the new SDK key
        """
        return Config(sdk_key=new_sdk_key,
                      base_uri=self.__base_uri,
                      events_uri=self.__events_uri,
                      events_max_pending=self.__events_max_pending,
                      flush_interval=self.__flush_interval,
                      stream_uri=self.__stream_uri,
                      stream=self.__stream,
                      initial_reconnect_delay=self.__initial_reconnect_delay,
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
                      wrapper_version=self.__wrapper_version,
                      http=self.__http)

    # for internal use only - probably should be part of the client logic
    def get_default(self, key, default):
        return default if key not in self.__defaults else self.__defaults[key]

    @property
    def sdk_key(self) -> Optional[str]:
        return self.__sdk_key

    @property
    def base_uri(self) -> str:
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
    def update_processor_class(self) -> Optional[Callable[[str, 'Config', FeatureStore], UpdateProcessor]]:
        return self.__update_processor_class

    @property
    def stream(self) -> bool:
        return self.__stream

    @property
    def initial_reconnect_delay(self) -> float:
        return self.__initial_reconnect_delay
    @property
    def poll_interval(self) -> float:
        return self.__poll_interval

    @property
    def use_ldd(self) -> bool:
        return self.__use_ldd

    @property
    def feature_store(self) -> FeatureStore:
        return self.__feature_store

    @property
    def event_processor_class(self) -> Optional[Callable[['Config'], EventProcessor]]:
        return self.__event_processor_class

    @property
    def feature_requester_class(self) -> Callable:
        return self.__feature_requester_class

    @property
    def events_enabled(self) -> bool:
        return self.__send_events

    @property
    def send_events(self) -> bool:
        return self.__send_events

    @property
    def events_max_pending(self) -> int:
        return self.__events_max_pending

    @property
    def flush_interval(self) -> float:
        return self.__flush_interval

    @property
    def private_attribute_names(self) -> list:
        return list(self.__private_attribute_names)

    @property
    def all_attributes_private(self) -> bool:
        return self.__all_attributes_private

    @property
    def offline(self) -> bool:
        return self.__offline

    @property
    def user_keys_capacity(self) -> int:
        return self.__user_keys_capacity

    @property
    def user_keys_flush_interval(self) -> float:
        return self.__user_keys_flush_interval

    @property
    def inline_users_in_events(self) -> bool:
        return self.__inline_users_in_events

    @property
    def diagnostic_opt_out(self) -> bool:
        return self.__diagnostic_opt_out

    @property
    def diagnostic_recording_interval(self) -> int:
        return self.__diagnostic_recording_interval

    @property
    def wrapper_name(self) -> Optional[str]:
        return self.__wrapper_name

    @property
    def wrapper_version(self) -> Optional[str]:
        return self.__wrapper_version

    @property
    def http(self) -> HTTPConfig:
        return self.__http

    def _validate(self):
        if self.offline is False and self.sdk_key is None or self.sdk_key == '':
            log.warning("Missing or blank sdk_key.")
