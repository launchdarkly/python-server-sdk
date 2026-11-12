"""
This submodule contains the :class:`Config` class for custom configuration of the SDK client.

Note that the same class can also be imported from the ``ldclient.client`` submodule.
"""

import copy
from dataclasses import dataclass
from typing import Callable, List, Optional, Protocol, Set, TypeVar

from ldclient.hook import Hook
from ldclient.impl.util import (
    log,
    validate_application_info,
    validate_sdk_key_format
)
from ldclient.interfaces import (
    BigSegmentStore,
    DataStoreMode,
    EventProcessor,
    FeatureStore,
    Initializer,
    Synchronizer
)
from ldclient.plugin import Plugin

DEFAULT_BASE_URI = 'https://app.launchdarkly.com'
DEFAULT_EVENTS_URI = 'https://events.launchdarkly.com'
DEFAULT_STREAM_URI = 'https://stream.launchdarkly.com'

# Defaults, in seconds, for the two configurable data source intervals. The
# poll interval is also its own minimum.
DEFAULT_INITIAL_RECONNECT_DELAY = 1
DEFAULT_POLL_INTERVAL = 30


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
    """Advanced HTTP configuration options for the SDK client / data sources.

    This class groups together HTTP/HTTPS-related configuration properties that rarely need to be changed.
    If you need to set these, construct an ``HTTPConfig`` instance and pass it as the ``http`` parameter when
    you construct the main :class:`Config` for the SDK client, or to the appropriate data source builder method.
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


T_co = TypeVar("T_co", covariant=True)


class SdkIdentityConfig(Protocol):
    """
    The SDK's self-identity fields — read by the shared environment-metadata and
    secure-mode helpers, and included in request headers.
    """

    @property
    def sdk_key(self) -> Optional[str]:
        """The SDK key used to authenticate requests."""

    @property
    def application(self) -> dict:
        """The application metadata included in request headers."""

    @property
    def wrapper_name(self) -> Optional[str]:
        """The wrapper name included in request headers."""

    @property
    def wrapper_version(self) -> Optional[str]:
        """The wrapper version included in request headers."""


class DataSourceBuilderConfig(SdkIdentityConfig, Protocol):
    """
    The subset of configuration that data source builders and the data sources
    they build read. Extends :class:`SdkIdentityConfig` with the transport and
    endpoint fields. It is the structural contract a config object must satisfy
    to be passed to :meth:`DataSourceBuilder.build`.
    """

    @property
    def base_uri(self) -> str:
        """The base URI for polling requests."""

    @property
    def stream_base_uri(self) -> str:
        """The base URI for streaming requests."""

    @property
    def http(self) -> HTTPConfig:
        """The HTTP configuration used for requests."""

    @property
    def initial_reconnect_delay(self) -> float:
        """The initial reconnect delay for the streaming data source."""

    @property
    def poll_interval(self) -> float:
        """The interval between polling requests."""

    # Set by the client after construction, so it is a read-write attribute
    # rather than a read-only property.
    _instance_id: Optional[str]


class PrivateAttributesConfig(Protocol):
    """
    The private-attribute redaction settings read by the event output formatter
    when building analytics events.
    """

    @property
    def all_attributes_private(self) -> bool:
        """Whether all context attributes should be treated as private."""

    @property
    def private_attributes(self) -> List[str]:
        """The context attribute references to treat as private."""


class DataSourceBuilder(Protocol[T_co]):  # pylint: disable=too-few-public-methods
    """
    Protocol for building data sources.
    """

    def build(self, config: DataSourceBuilderConfig) -> T_co:
        """
        Builds the data source.

        :param config: the SDK configuration
        :return: the built data source
        """
        raise NotImplementedError


@dataclass(frozen=True)
class DataSystemConfig:
    """Configuration for LaunchDarkly's data acquisition strategy."""

    initializers: Optional[List[DataSourceBuilder[Initializer]]]
    """The initializers for the data system."""

    synchronizers: Optional[List[DataSourceBuilder[Synchronizer]]]
    """
    The synchronizers for the data system, ordered by preference.
    The first synchronizer is the most preferred, with subsequent synchronizers
    serving as fallbacks in order of decreasing preference.
    """

    data_store_mode: DataStoreMode = DataStoreMode.READ_WRITE
    """The data store mode specifies the mode in which the persistent store will operate, if present."""

    data_store: Optional[FeatureStore] = None
    """The (optional) persistent data store instance."""

    fdv1_fallback_synchronizer: Optional[DataSourceBuilder[Synchronizer]] = None
    """An optional fallback synchronizer that will read from FDv1"""


class Config(DataSourceBuilderConfig, PrivateAttributesConfig):
    """Advanced configuration options for the SDK client.

    To use these options, create an instance of ``Config`` and pass it to either :func:`ldclient.set_config()`
    if you are using the singleton client, or the :class:`ldclient.client.LDClient` constructor otherwise.
    """

    def __init__(
        self,
        sdk_key: str,
        *,
        base_uri: str = DEFAULT_BASE_URI,
        events_uri: str = DEFAULT_EVENTS_URI,
        events_max_pending: int = 10000,
        flush_interval: float = 5,
        stream_uri: str = DEFAULT_STREAM_URI,
        initial_reconnect_delay: float = DEFAULT_INITIAL_RECONNECT_DELAY,
        defaults: dict = {},
        send_events: Optional[bool] = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
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
        plugins: Optional[List[Plugin]] = None,
        enable_event_compression: bool = True,
        omit_anonymous_contexts: bool = False,
        data_system: Optional[DataSystemConfig] = None,
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
        :param array private_attributes: Marks a set of attributes private. Any users sent to LaunchDarkly
          with this configuration active will have these attributes removed. Each item can be either the
          name of an attribute ("email"), or a slash-delimited path ("/address/street") to mark a
          property within a JSON object value as private.
        :param all_attributes_private: If true, all user attributes (other than the key) will be
          private, not just the attributes specified in ``private_attributes``.
        :param context_keys_capacity: The number of context keys that the event processor can remember at any
          one time, so that duplicate context details will not be sent in analytics events.
        :param context_keys_flush_interval: The interval in seconds at which the event processor will
          reset its set of known context keys.
        :param event_processor_class: A factory for an EventProcessor implementation taking the config
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
        :param plugins: A list of plugins to be used with the SDK. Plugin support is currently experimental and subject to change.
        :param enable_event_compression: Whether to use GZIP compression for the event payloads that the SDK
          sends. The default is True. Set this to False if you send events to a Relay Proxy older than
          version 8.9, because those versions cannot accept compressed events. Relay Proxy users can also
          set this to False to prevent extra CPU use, because the Relay Proxy decompresses and recompresses
          the payloads.
        :param omit_anonymous_contexts: Sets whether anonymous contexts should be omitted from index and identify events.
        :param data_system: Configuration for the data system, which controls how the SDK gets flag and
          segment data. Build it with the functions in :mod:`ldclient.datasystem`. If it is not set, the SDK
          uses :func:`ldclient.datasystem.default`.
        """
        self.__sdk_key = validate_sdk_key_format(sdk_key, log)

        self.__base_uri = base_uri.rstrip('/')
        self.__events_uri = events_uri.rstrip('/')
        self.__stream_uri = stream_uri.rstrip('/')
        self.__initial_reconnect_delay = initial_reconnect_delay
        self.__poll_interval = max(poll_interval, DEFAULT_POLL_INTERVAL)
        self.__event_processor_class = event_processor_class
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
        self.__plugins = [plugin for plugin in plugins if isinstance(plugin, Plugin)] if plugins else []
        self.__enable_event_compression = enable_event_compression
        self.__omit_anonymous_contexts = omit_anonymous_contexts
        self._instance_id: Optional[str] = None
        self._data_system = data_system

    def with_wrapper_information(self, wrapper_name: Optional[str], wrapper_version: Optional[str] = None) -> 'Config':
        """Returns a new ``Config`` instance that is the same as this one, except for having different wrapper information.

        This is intended for use by wrapper libraries, such as the LaunchDarkly OpenFeature providers, which need to
        identify themselves rather than the application that configured the client.

        The new instance is a shallow copy: it shares the objects the original configuration references, such as the
        data system configuration, the hooks, and the HTTP configuration. Mutating one of those objects affects both
        configurations.

        :param wrapper_name: an identifying name for the wrapper being used; see :py:attr:`~wrapper_name`
        :param wrapper_version: the version of the wrapper being used; see :py:attr:`~wrapper_version`
        """
        updated = copy.copy(self)
        updated.__wrapper_name = wrapper_name
        updated.__wrapper_version = wrapper_version

        return updated

    # for internal use only - probably should be part of the client logic
    def get_default(self, key, default):
        return default if key not in self.__defaults else self.__defaults[key]

    @property
    def sdk_key(self) -> Optional[str]:
        return self.__sdk_key

    @property
    def base_uri(self) -> str:
        return self.__base_uri

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

    @property
    def initial_reconnect_delay(self) -> float:
        return self.__initial_reconnect_delay

    @property
    def poll_interval(self) -> float:
        return self.__poll_interval

    @property
    def event_processor_class(self) -> Optional[Callable[['Config'], EventProcessor]]:
        return self.__event_processor_class

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
    def plugins(self) -> List[Plugin]:
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
    def data_system(self) -> Optional[DataSystemConfig]:
        """
        Configuration for the data system, which controls how the SDK gets
        flag and segment data.
        """
        return self._data_system

    def _validate(self):
        if self.offline is False and self.sdk_key == '':
            log.warning("Missing or blank SDK key")


__all__ = ['Config', 'BigSegmentsConfig', 'DataSourceBuilder', 'DataSystemConfig', 'HTTPConfig']
