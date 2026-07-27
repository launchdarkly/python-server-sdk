"""
This submodule contains the :class:`AsyncConfig` class for custom configuration of
the async SDK client.

.. caution::
    This feature is experimental and should NOT be considered ready for production
    use. It may change or be removed without notice and is not subject to backwards
    compatibility guarantees.
"""

from typing import TYPE_CHECKING, Callable, List, Optional, Set

from ldclient.config import (
    GET_LATEST_FEATURES_PATH,
    STREAM_FLAGS_PATH,
    DataSourceBuilderConfig,
    DataSystemConfig,
    HTTPConfig,
    PrivateAttributesConfig
)
from ldclient.hook import AsyncHook
from ldclient.impl.aio.concurrency import AsyncEvent
from ldclient.impl.util import (
    log,
    validate_application_info,
    validate_sdk_key_format
)
from ldclient.interfaces import (
    AsyncBigSegmentStore,
    AsyncDataSourceUpdateSink,
    AsyncFeatureStore,
    UpdateProcessor
)
from ldclient.plugin import AsyncPlugin

if TYPE_CHECKING:
    # Imported for typing only. The concrete AsyncEventProcessor pulls in aiohttp
    # transitively, so it is kept out of the runtime import graph.
    from ldclient.impl.events.async_event_processor import AsyncEventProcessor


class AsyncBigSegmentsConfig:
    """Configuration options related to Big Segments for the async SDK client.

    Big Segments are a specific type of segments. For more information, read the LaunchDarkly
    documentation: https://docs.launchdarkly.com/home/users/big-segments

    If your application uses Big Segments, you will need to create an ``AsyncBigSegmentsConfig``
    that at a minimum specifies what database integration to use, and then pass the
    ``AsyncBigSegmentsConfig`` object as the ``big_segments`` parameter when creating an
    :class:`AsyncConfig`.
    """

    def __init__(self, store: Optional[AsyncBigSegmentStore] = None, context_cache_size: int = 1000, context_cache_time: float = 5, status_poll_interval: float = 5, stale_after: float = 120):
        """
        :param store: the implementation of :class:`ldclient.interfaces.AsyncBigSegmentStore` that
            will be used to query the Big Segments database
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

    @property
    def store(self) -> Optional[AsyncBigSegmentStore]:
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


class AsyncConfig(DataSourceBuilderConfig, PrivateAttributesConfig):
    """Advanced configuration options for the async SDK client.

    To use these options, create an instance of ``AsyncConfig`` and pass it to the
    :class:`ldclient.async_client.AsyncLDClient` constructor.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees.
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
        update_processor_class: Optional[Callable[['AsyncConfig', AsyncFeatureStore, AsyncEvent], UpdateProcessor]] = None,
        poll_interval: float = 30,
        use_ldd: bool = False,
        feature_store: Optional[AsyncFeatureStore] = None,
        feature_requester_class=None,
        event_processor_class: Optional[Callable[['AsyncConfig'], 'AsyncEventProcessor']] = None,
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
        big_segments: Optional[AsyncBigSegmentsConfig] = None,
        application: Optional[dict] = None,
        hooks: Optional[List[AsyncHook]] = None,
        plugins: Optional[List[AsyncPlugin]] = None,
        enable_event_compression: bool = False,
        omit_anonymous_contexts: bool = False,
        payload_filter_key: Optional[str] = None,
        datasystem_config: Optional[DataSystemConfig] = None,
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
        :param feature_store: An AsyncFeatureStore implementation
        :param context_keys_capacity: The number of context keys that the event processor can remember at any
          one time, so that duplicate context details will not be sent in analytics events.
        :param context_keys_flush_interval: The interval in seconds at which the event processor will
          reset its set of known context keys.
        :param feature_requester_class: A factory for a FeatureRequester implementation taking the sdk key and config
        :param event_processor_class: A factory for an AsyncEventProcessor implementation taking the config
        :param update_processor_class: A factory for an UpdateProcessor implementation taking the config, an
            AsyncFeatureStore implementation, and an AsyncEvent to signal readiness.
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
          :class:`ldclient.config.HTTPConfig`.
        :param application: Optional properties for setting application metadata. See :py:attr:`~application`
        :param hooks: Hooks provide entrypoints which allow for observation of SDK functions.
        :param plugins: A list of plugins to be used with the SDK. Plugin support is currently experimental and subject to change.
        :param enable_event_compression: Whether or not to enable GZIP compression for outgoing events.
        :param omit_anonymous_contexts: Sets whether anonymous contexts should be omitted from index and identify events.
        :param payload_filter_key: The payload filter is used to selectively limited the flags and segments delivered in the data source payload.
        :param datasystem_config: Configuration for the upcoming enhanced data system design. This is experimental and should not be set without direction from LaunchDarkly support.
        """
        self.__sdk_key = validate_sdk_key_format(sdk_key, log)

        self.__base_uri = base_uri.rstrip('/')
        self.__events_uri = events_uri.rstrip('/')
        self.__stream_uri = stream_uri.rstrip('/')
        self.__update_processor_class = update_processor_class
        self.__stream = stream
        self.__initial_reconnect_delay = initial_reconnect_delay
        self.__poll_interval = max(poll_interval, 30.0)
        self.__use_ldd = use_ldd
        self.__feature_store = feature_store
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
        self.__big_segments = AsyncBigSegmentsConfig() if not big_segments else big_segments
        self.__application = validate_application_info(application or {}, log)
        self.__hooks = [hook for hook in hooks if isinstance(hook, AsyncHook)] if hooks else []
        self.__plugins = [plugin for plugin in plugins if isinstance(plugin, AsyncPlugin)] if plugins else []
        self.__enable_event_compression = enable_event_compression
        self.__omit_anonymous_contexts = omit_anonymous_contexts
        self.__payload_filter_key = payload_filter_key
        self._data_source_update_sink: Optional[AsyncDataSourceUpdateSink] = None
        self.__instance_id: Optional[str] = None
        self._datasystem_config = datasystem_config

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
    def stream_base_uri(self) -> str:
        return self.__stream_uri

    # for internal use only - should construct the URL path in the streaming code, not here
    @property
    def stream_uri(self):
        return self.__stream_uri + STREAM_FLAGS_PATH

    @property
    def update_processor_class(self) -> Optional[Callable[['AsyncConfig', AsyncFeatureStore, AsyncEvent], UpdateProcessor]]:
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
    def feature_store(self) -> Optional[AsyncFeatureStore]:
        return self.__feature_store

    @property
    def event_processor_class(self) -> Optional[Callable[['AsyncConfig'], 'AsyncEventProcessor']]:
        return self.__event_processor_class

    @property
    def feature_requester_class(self) -> Optional[Callable]:
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
    def big_segments(self) -> AsyncBigSegmentsConfig:
        return self.__big_segments

    @property
    def application(self) -> dict:
        """
        An object that allows configuration of application metadata.

        Application metadata may be used in LaunchDarkly analytics or other
        product features, but does not affect feature flag evaluations.

        If you want to set non-default values for any of these fields, provide
        the appropriately configured dict to the {AsyncConfig} object.
        """
        return self.__application

    @property
    def hooks(self) -> List[AsyncHook]:
        """
        Initial set of hooks for the client.

        Hooks provide entrypoints which allow for observation of SDK functions.

        LaunchDarkly provides integration packages, and most applications will
        not need to implement their own hooks.
        """
        return self.__hooks

    @property
    def plugins(self) -> List[AsyncPlugin]:
        """
        Initial set of plugins for the client.

        LaunchDarkly provides plugin packages, and most applications will
        not need to implement their own plugins.
        """
        return self.__plugins

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
    def _instance_id(self) -> Optional[str]:
        """The instance ID included in request headers. Set by the SDK."""
        return self.__instance_id

    @_instance_id.setter
    def _instance_id(self, value: Optional[str]) -> None:
        self.__instance_id = value

    @property
    def data_source_update_sink(self) -> Optional[AsyncDataSourceUpdateSink]:
        """
        Returns the component that allows a data source to push data into the SDK.

        This property should only be set by the SDK. Long term access of this
        property is not supported; it is temporarily being exposed to maintain
        backwards compatibility while the SDK structure is updated.

        Custom data source implementations should integrate with this sink if
        they want to provide support for data source status listeners.
        """
        return self._data_source_update_sink

    @property
    def datasystem_config(self) -> Optional[DataSystemConfig]:
        """
        Configuration for the upcoming enhanced data system design. This is
        experimental and should not be set without direction from LaunchDarkly
        support.
        """
        return self._datasystem_config

    def _validate(self):
        if self.offline is False and self.sdk_key == '':
            log.warning("Missing or blank SDK key")


__all__ = ['AsyncConfig', 'AsyncBigSegmentsConfig']
