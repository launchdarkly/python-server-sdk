from os import environ
from typing import Optional, Tuple
from urllib.parse import urlparse

import certifi
import urllib3

from ldclient.version import VERSION


def _application_header_value(application: dict) -> str:
    parts = []
    id = application.get('id', '')
    version = application.get('version', '')

    if id:
        parts.append("application-id/%s" % id)

    if version:
        parts.append("application-version/%s" % version)

    return " ".join(parts)


def _base_headers(config):
    headers = {'Authorization': config.sdk_key or '', 'User-Agent': 'PythonClient/' + VERSION}

    app_value = _application_header_value(config.application)
    if app_value:
        headers['X-LaunchDarkly-Tags'] = app_value

    if isinstance(config.wrapper_name, str) and config.wrapper_name != "":
        wrapper_version = ""
        if isinstance(config.wrapper_version, str) and config.wrapper_version != "":
            wrapper_version = "/" + config.wrapper_version
        headers.update({'X-LaunchDarkly-Wrapper': config.wrapper_name + wrapper_version})

    return headers


def _http_factory(config):
    return HTTPFactory(_base_headers(config), config.http)


class HTTPFactory:
    def __init__(self, base_headers, http_config, override_read_timeout=None):
        self.__base_headers = base_headers
        self.__http_config = http_config
        self.__timeout = urllib3.Timeout(connect=http_config.connect_timeout, read=http_config.read_timeout if override_read_timeout is None else override_read_timeout)

    @property
    def base_headers(self):
        return self.__base_headers

    @property
    def http_config(self):
        return self.__http_config

    @property
    def timeout(self):
        return self.__timeout

    def create_pool_manager(self, num_pools, target_base_uri):
        proxy_url = self.__http_config.http_proxy or _get_proxy_url(target_base_uri)

        if self.__http_config.disable_ssl_verification:
            cert_reqs = 'CERT_NONE'
            ca_certs = None
        else:
            cert_reqs = 'CERT_REQUIRED'
            ca_certs = self.__http_config.ca_certs or certifi.where()

        if proxy_url is None:
            return urllib3.PoolManager(num_pools=num_pools, cert_reqs=cert_reqs, ca_certs=ca_certs)
        else:
            # Get proxy authentication, if provided
            url = urllib3.util.parse_url(proxy_url)
            proxy_headers = None
            if url.auth is not None:
                proxy_headers = urllib3.util.make_headers(proxy_basic_auth=url.auth)
            # Create a proxied connection
            return urllib3.ProxyManager(proxy_url, num_pools=num_pools, cert_reqs=cert_reqs, ca_certs=ca_certs, proxy_headers=proxy_headers)


def _get_proxy_url(target_base_uri):
    """
    Determine the proxy URL to use for a given target URI, based on the
    environment variables http_proxy, https_proxy, and no_proxy.

    If the target URI is an https URL, the proxy will be determined from the HTTPS_PROXY variable.
    If the target URI is not https, the proxy will be determined from the HTTP_PROXY variable.

    In either of the above instances, if the NO_PROXY variable contains either
    the target domain or '*', no proxy will be used.
    """
    if target_base_uri is None:
        return None

    target_host, target_port, is_https = _get_target_host_and_port(target_base_uri)

    proxy_url = environ.get('https_proxy') if is_https else environ.get('http_proxy')
    no_proxy = environ.get('no_proxy', '').strip()

    if proxy_url is None or no_proxy == '*':
        return None
    elif no_proxy == '':
        return proxy_url

    for no_proxy_entry in no_proxy.split(','):
        if no_proxy_entry == '':
            continue
        parts = no_proxy_entry.strip().split(':')
        if len(parts) == 1:
            if target_host.endswith(no_proxy_entry):
                return None
            continue
        if parts[0] == '':
            continue
        if target_host.endswith(parts[0]) and target_port == int(parts[1]):
            return None

    return proxy_url


def _get_target_host_and_port(uri: str) -> Tuple[str, int, bool]:
    """
    Given a URL, return the effective hostname, port, and whether it is considered a secure scheme.

    If a scheme is not supplied, the port is assumed to be 80 and the connection unsecure.
    If a scheme and port is provided, the port will be parsed from the URI.
    If only a scheme is provided, the port will be 443 if the scheme is 'https', otherwise 80.
    """
    if '//' not in uri:
        parts = uri.split(':')
        return parts[0], int(parts[1]) if len(parts) > 1 else 80, False

    parsed = urlparse(uri)
    is_https = parsed.scheme == 'https'

    port = parsed.port
    if port is None:
        port = 443 if is_https else 80

    return parsed.hostname or "", port, is_https
