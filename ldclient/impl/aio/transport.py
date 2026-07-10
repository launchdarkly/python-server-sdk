"""
Async HTTP transport wrapping an aiohttp ``ClientSession``, exposing a
``TransportResponse`` so callers can inspect a response after the request
context has closed. The sync side talks to urllib3 directly.
"""

import ssl
from typing import Optional, Union

import aiohttp
import certifi
from ld_eventsource.async_client import AsyncSSEClient
from ld_eventsource.config.async_connect_strategy import AsyncConnectStrategy
from ld_eventsource.config.error_strategy import ErrorStrategy
from ld_eventsource.config.retry_delay_strategy import RetryDelayStrategy

from ldclient.impl.aio.transport_types import TransportResponse
from ldclient.impl.http import _base_headers
from ldclient.impl.util import log

# Allows up to 5 minutes to elapse without any data sent across the stream.
# Heartbeats sent as comments will keep this from triggering.
STREAM_READ_TIMEOUT = 5 * 60

MAX_RETRY_DELAY = 30
BACKOFF_RESET_INTERVAL = 60
JITTER_RATIO = 0.5


def make_client_session(config, http_options=None) -> aiohttp.ClientSession:
    """Creates an ``aiohttp.ClientSession`` configured from the SDK config's
    HTTP options (CA certs, client certs, SSL verification, proxy trust).
    ``http_options`` overrides the config's HTTP options when given."""
    http_config = http_options if http_options is not None else config.http
    ssl_ctx = ssl.create_default_context(cafile=http_config.ca_certs or certifi.where())
    if http_config.cert_file:
        ssl_ctx.load_cert_chain(http_config.cert_file)
    if http_config.disable_ssl_verification:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        log.warning("TLS verification disabled")

    connector = aiohttp.TCPConnector(ssl=ssl_ctx, limit_per_host=10)
    return aiohttp.ClientSession(
        connector=connector,
        trust_env=(http_config.http_proxy is None),
    )


class AsyncHTTPTransport:
    """Performs HTTP requests over an aiohttp ``ClientSession``.

    If no session is supplied, one is created lazily from the config on first
    use and owned (closed) by the transport; a supplied session remains owned
    by the caller. ``http_options`` overrides the config's HTTP options for
    lazy session construction and default request timeouts.
    """

    def __init__(
        self,
        config,
        client: Optional[aiohttp.ClientSession] = None,
        http_options=None,
    ):
        self._config = config
        self._http_options = http_options if http_options is not None else config.http
        self._client = client
        self._owns_client = client is None
        self._proxy = self._http_options.http_proxy or None

    async def request(
        self,
        method: str,
        uri: str,
        headers: Optional[dict] = None,
        body: Optional[Union[bytes, str]] = None,
        connect_timeout: Optional[float] = None,
        read_timeout: Optional[float] = None,
    ) -> TransportResponse:
        """Performs a request."""
        if self._client is None:
            self._client = make_client_session(self._config, self._http_options)
        timeout = aiohttp.ClientTimeout(
            connect=connect_timeout if connect_timeout is not None else self._http_options.connect_timeout,
            sock_read=read_timeout if read_timeout is not None else self._http_options.read_timeout,
        )
        async with self._client.request(
            method,
            uri,
            headers=headers,
            data=body,
            timeout=timeout,
            proxy=self._proxy,
        ) as response:
            text = await response.text(encoding='UTF-8', errors='replace')
            return TransportResponse(response.status, response.headers, text)

    async def close(self) -> None:
        """Closes the underlying session if this transport created it."""
        if self._owns_client and self._client is not None:
            await self._client.close()
            self._client = None


class AsyncSSEFactory:
    """Creates configured ``AsyncSSEClient`` instances for streaming connections.

    A supplied aiohttp session remains owned by the caller; the SSE client
    never closes it. Callers are expected to supply a session built from the
    SDK's HTTP options (via ``make_client_session``) so the streaming
    connection uses the configured certs, SSL settings, and proxy trust.
    ``http_options`` overrides the config's HTTP options for connection
    timeouts and proxy settings.
    """

    def __init__(self, config, session: Optional[aiohttp.ClientSession] = None, proxy: Optional[str] = None, http_options=None):
        self._config = config
        self._session = session
        self._http_options = http_options if http_options is not None else config.http
        self._proxy = proxy if proxy is not None else (self._http_options.http_proxy or None)

    def create(self, url: str, initial_retry_delay: float, query_params=None) -> AsyncSSEClient:
        """Builds an SSE client for the given stream URL. Headers, timeouts,
        proxy settings, and the retry/backoff policy come from the SDK config.
        ``query_params`` is an optional zero-argument callable evaluated on
        each (re)connect to produce additional query string parameters."""
        base_headers = _base_headers(self._config)
        aiohttp_request_options: dict = {
            "timeout": aiohttp.ClientTimeout(
                total=None,
                connect=self._http_options.connect_timeout,
                sock_read=STREAM_READ_TIMEOUT,
            )
        }
        if self._proxy:
            aiohttp_request_options["proxy"] = self._proxy
        return AsyncSSEClient(
            connect=AsyncConnectStrategy.http(
                url=url,
                headers=base_headers,
                session=self._session,
                aiohttp_request_options=aiohttp_request_options,
                query_params=query_params,
            ),
            error_strategy=ErrorStrategy.always_continue(),  # we'll make error-handling decisions when we see a Fault
            initial_retry_delay=initial_retry_delay,
            retry_delay_strategy=RetryDelayStrategy.default(
                max_delay=MAX_RETRY_DELAY,
                backoff_multiplier=2,
                jitter_multiplier=JITTER_RATIO,
            ),
            retry_delay_reset_threshold=BACKOFF_RESET_INTERVAL,
            logger=log,
        )
