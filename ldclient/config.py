"""
This submodule contains the :class:`Config` class for custom configuration of the SDK client.

Note that the same class can also be imported from the ``ldclient.client`` submodule.
"""

from threading import Event
from typing import Callable, List, Optional, Set

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.hook import Hook
from ldclient.impl.util import log, validate_application_info
from ldclient.interfaces import (BigSegmentStore, DataSourceUpdateSink,
                                 EventProcessor, FeatureStore, UpdateProcessor)

GET_LATEST_FEATURES_PATH = '/sdk/latest-flags'
STREAM_FLAGS_PATH = '/flags'


class BigSegmentsConfig:
    """Configuration options related to Big Segments.

    Big Segments are a specific type of segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments

    If your application uses Big Segments, you will need to create a ``BigSegmentsConfig`` that at a
    minimum specifies what database integration to use, and then pass the ``BigSegmentsConfig``
    object as the ``big_segments`` parameter when creating a :class:`Config`.

    This example shows Big Segments being configured to use Redis:
    ::

            from ldclient.config import Config, BigSegmentsConfig
            from ldclient.integrations import Redis
            store = Redis.new_big_segment_store(url='redis://localhost:6379')
            config = Config(big_segments=BigSegmentsConfig(store = store))
    """

    def __init__(self, store: Optional[BigSegmentStore] = None, context_cache_size: int = 1000, context_cache_time: float = 5, status_poll_interval: float = 5, stale_after: float = 120):
        """
        :param store: the implementation of :class:`ldclient.interfaces.BigSegmentStore` that will
            be used to query the Big Segments database
        :param context_cache_size: the maximum number of contexts whose Big Segment state will be cached
            by the SDK at any given time
        :param context_cache_time: the maximum length of time (in seconds) that the Big Segment state
            for a context will be cached by the SDK
        :param status_poll_interval: the interval (in seconds) at which the SDK will poll the Big
            Segment store to make sure it is available and to determine how long ago it was updated
        :param stale_after: the maximum length of time between updates of the Big Segments data
            before the data is considered out of date
        """
        self.__store = store
        self.__context_cache_size = context_cache_size
        self.__context_cache_time = context_cache_time
        self.__status_poll_interval = status_poll_interval
        self.__stale_after = stale_after
        pass

    @property
    def store(self) -> Optional[BigSegmentStore]:
        return self.__store

    @property
    def context_cache_size(self) -> int:
        return self.__context_cache_size

    @property
    def context_cache_time(self) -> float:
        return self.__context_cache_time

    @property
    def status_poll_interval(self) -> float:
        return self.__status_poll_interval

    @property
    def stale_after(self) -> float:
        return self.__stale_after


class HTTPConfig:
    """Advanced HTTP configuration options for the SDK client.

    This class groups together HTTP/HTTPS-related configuration properties that rarely need to be changed.
    If you need to set these, construct an ``HTTPConfig`` instance and pass it as the ``http`` parameter when
    you construct the main :class:`Config` for the SDK client.
    """

    def __init__(
        self,
        connect_timeout: float = 10,
        read_timeout: float = 15,
        http_proxy: Optional[str] = None,
        ca_certs: Optional[str] = None,
        cert_file: Optional[str] = None,
        disable_ssl_verification: bool = False,
    ):
        """
        :param connect_timeout: The connect timeout for network connections in seconds.
        :param read_timeout: The read timeout for network connections in seconds.
        :param http_proxy: Use a proxy when connecting to LaunchDarkly. This is the full URI of the
          proxy; for example: http://my-proxy.com:1234. Note that unlike the standard ``http_proxy`` environment
          variable, this is used regardless of whether the target URI is HTTP or HTTPS (the actual LaunchDarkly
          service uses HTTPS, but a Relay Proxy instance could use HTTP). Setting this Config parameter will
          override any proxy specified by an environment variable, but only for LaunchDarkly SDK connections.
        :param ca_certs: If using a custom certificate authority, set this to the file path of the
          certificate bundle.
        :param cert_file: If using a custom client certificate, set this to the file path of the
          certificate.
        :param disable_ssl_verification: If true, completely disables SSL verification and certificate
          verification for secure requests. This is unsafe and should not be used in a production environment;
          instead, use a self-signed certificate and set ``ca_certs``.
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

    def __init__(
        self,
        sdk_key: str,
        base_uri: str = 'https://app.launchdarkly.com',
        events_uri: str = 'https://events.launchdarkly.com',
        events_max_pending: int = 10000,
        flush_interval: float = 5,
        stream_uri: str = 'https://stream.launchdarkly.com',
        stream: bool = True,
        initial_reconnect_delay: float = 1,
        defaults: dict = {},
        send_events: Optional[bool] = None,
        update_processor_class: Optional[Callable[['Config', FeatureStore, Event], UpdateProcessor]] = None,
        poll_interval: float = 30,
        use_ldd: bool = False,
        feature_store: Optional[FeatureStore] = None,
        feature_requester_class=None,
        event_processor_class: Optional[Callable[['Config'], EventProcessor]] = None,
        private_attributes: Set[str] = set(),
        all_attributes_private: bool = False,
        offline: bool = False,
        context_keys_capacity: int = 1000,
        context_keys_flush_interval: float = 300,
        diagnostic_opt_out: bool = False,
        diagnostic_recording_interval: int = 900,
        wrapper_name: Optional[str] = None,
        wrapper_version: Optional[str] = None,
        http: HTTPConfig = HTTPConfig(),
        big_segments: Optional[BigSegmentsConfig] = None,
        application: Optional[dict] = None,
        hooks: Optional[List[Hook]] = None,
        enable_event_compression: bool = False,
        omit_anonymous_contexts: bool = False,
        payload_filter_key: Optional[str] = None,
    ):
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
          ``offline`` in that it affects only the sending of client-side events, not streaming or polling for
          events from the server. By default, events will be sent.
        :param offline: Whether the client should be initialized in offline mode. In offline mode,
          default values are returned for all flags and no remote network requests are made. By default,
          this is false.
        :param poll_interval: The number of seconds between polls for flag updates if streaming is off.
        :param use_ldd: Whether you are using the LaunchDarkly Relay Proxy in daemon mode. In this
          configuration, the client will not use a streaming connection to listen for updates, but instead
          will get feature state from a Redis instance. The ``stream`` and ``poll_interval`` options will be
          ignored if this option is set to true. By default, this is false.
          For more information, read the LaunchDarkly
          documentation: https://docs.launchdarkly.com/home/relay-proxy/using#using-daemon-mode
        :param array private_attributes: Marks a set of attributes private. Any users sent to LaunchDarkly
          with this configuration active will have these attributes removed. Each item can be either the
          name of an attribute ("email"), or a slash-delimited path ("/address/street") to mark a
          property within a JSON object value as private.
        :param all_attributes_private: If true, all user attributes (other than the key) will be
          private, not just the attributes specified in ``private_attributes``.
        :param feature_store: A FeatureStore implementation
        :param context_keys_capacity: The number of context keys that the event processor can remember at any
          one time, so that duplicate context details will not be sent in analytics events.
        :param context_keys_flush_interval: The interval in seconds at which the event processor will
          reset its set of known context keys.
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :param event_processor_class: A factory for an EventProcessor implementation taking the config
        :param update_processor_class: A factory for an UpdateProcessor implementation taking the config, a FeatureStore
            implementation, and a threading `Event` to signal readiness.
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
          use. If ``wrapper_name`` is not set, this field will be ignored. Otherwise the version string will
          be included in the HTTP headers along with the ``wrapper_name`` during requests to the LaunchDarkly
          servers.
        :param http: Optional properties for customizing the client's HTTP/HTTPS behavior. See
          :class:`HTTPConfig`.
        :param application: Optional properties for setting application metadata. See :py:attr:`~application`
        :param hooks: Hooks provide entrypoints which allow for observation of SDK functions.
        :param enable_event_compression: Whether or not to enable GZIP compression for outgoing events.
        :param omit_anonymous_contexts: Sets whether anonymous contexts should be omitted from index and identify events.
        :param payload_filter_key: The payload filter is used to selectively limited the flags and segments delivered in the data source payload.
        """
        self.__sdk_key = sdk_key

        self.__base_uri = base_uri.rstrip('/')
        self.__events_uri = events_uri.rstrip('/')
        self.__stream_uri = stream_uri.rstrip('/')
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
        self.__send_events = True if send_events is None else send_events
        self.__private_attributes = private_attributes
        self.__all_attributes_private = all_attributes_private
        self.__offline = offline
        self.__context_keys_capacity = context_keys_capacity
        self.__context_keys_flush_interval = context_keys_flush_interval
        self.__diagnostic_opt_out = diagnostic_opt_out
        self.__diagnostic_recording_interval = max(diagnostic_recording_interval, 60)
        self.__wrapper_name = wrapper_name
        self.__wrapper_version = wrapper_version
        self.__http = http
        self.__big_segments = BigSegmentsConfig() if not big_segments else big_segments
        self.__application = validate_application_info(application or {}, log)
        self.__hooks = [hook for hook in hooks if isinstance(hook, Hook)] if hooks else []
        self.__enable_event_compression = enable_event_compression
        self.__omit_anonymous_contexts = omit_anonymous_contexts
        self.__payload_filter_key = payload_filter_key
        self._data_source_update_sink: Optional[DataSourceUpdateSink] = None

    def copy_with_new_sdk_key(self, new_sdk_key: str) -> 'Config':
        """Returns a new ``Config`` instance that is the same as this one, except for having a different SDK key.

        :param new_sdk_key: the new SDK key
        """
        return Config(
            sdk_key=new_sdk_key,
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
            private_attributes=self.__private_attributes,
            all_attributes_private=self.__all_attributes_private,
            offline=self.__offline,
            context_keys_capacity=self.__context_keys_capacity,
            context_keys_flush_interval=self.__context_keys_flush_interval,
            diagnostic_opt_out=self.__diagnostic_opt_out,
            diagnostic_recording_interval=self.__diagnostic_recording_interval,
            wrapper_name=self.__wrapper_name,
            wrapper_version=self.__wrapper_version,
            http=self.__http,
            big_segments=self.__big_segments,
        )

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
    def update_processor_class(self) -> Optional[Callable[['Config', FeatureStore, Event], UpdateProcessor]]:
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
    def send_events(self) -> bool:
        return self.__send_events

    @property
    def events_max_pending(self) -> int:
        return self.__events_max_pending

    @property
    def flush_interval(self) -> float:
        return self.__flush_interval

    @property
    def private_attributes(self) -> List[str]:
        return list(self.__private_attributes)

    @property
    def all_attributes_private(self) -> bool:
        return self.__all_attributes_private

    @property
    def offline(self) -> bool:
        return self.__offline

    @property
    def context_keys_capacity(self) -> int:
        return self.__context_keys_capacity

    @property
    def context_keys_flush_interval(self) -> float:
        return self.__context_keys_flush_interval

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

    @property
    def big_segments(self) -> BigSegmentsConfig:
        return self.__big_segments

    @property
    def application(self) -> dict:
        """
        An object that allows configuration of application metadata.

        Application metadata may be used in LaunchDarkly analytics or other
        product features, but does not affect feature flag evaluations.

        If you want to set non-default values for any of these fields, provide
        the appropriately configured dict to the {Config} object.
        """
        return self.__application

    @property
    def hooks(self) -> List[Hook]:
        """
        Initial set of hooks for the client.

        Hooks provide entrypoints which allow for observation of SDK functions.

        LaunchDarkly provides integration packages, and most applications will
        not need to implement their own hooks. Refer to the
        `launchdarkly-server-sdk-otel`.
        """
        return self.__hooks

    @property
    def enable_event_compression(self) -> bool:
        return self.__enable_event_compression

    @property
    def omit_anonymous_contexts(self) -> bool:
        """
        Determines whether or not anonymous contexts will be omitted from index and identify events.
        """
        return self.__omit_anonymous_contexts

    @property
    def payload_filter_key(self) -> Optional[str]:
        """
       LaunchDarkly Server SDKs historically downloaded all flag configuration
       and segments for a particular environment during initialization.

       For some customers, this is an unacceptably large amount of data, and
       has contributed to performance issues within their products.

       Filtered environments aim to solve this problem. By allowing customers
       to specify subsets of an environment's flags using a filter key, SDKs
       will initialize faster and use less memory.

       This payload filter key only applies to the default streaming and
       polling data sources. It will not affect TestData or FileData data
       sources, nor will it be applied to any data source provided through the
       {#data_source} config property.
        """
        return self.__payload_filter_key

    @property
    def data_source_update_sink(self) -> Optional[DataSourceUpdateSink]:
        """
        Returns the component that allows a data source to push data into the SDK.

        This property should only be set by the SDK. Long term access of this
        property is not supported; it is temporarily being exposed to maintain
        backwards compatibility while the SDK structure is updated.

        Custom data source implementations should integrate with this sink if
        they want to provide support for data source status listeners.
        """
        return self._data_source_update_sink

    def _validate(self):
        if self.offline is False and self.sdk_key is None or self.sdk_key == '':
            log.warning("Missing or blank sdk_key.")


__all__ = ['Config', 'BigSegmentsConfig', 'HTTPConfig']
