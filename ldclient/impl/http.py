from ldclient.version import VERSION
import certifi
from os import environ
import urllib3

def _base_headers(config):
    headers = {'Authorization': config.sdk_key or '',
               'User-Agent': 'PythonClient/' + VERSION}
    if isinstance(config.wrapper_name, str) and config.wrapper_name != "":
        wrapper_version = ""
        if isinstance(config.wrapper_version, str) and config.wrapper_version != "":
            wrapper_version = "/" + config.wrapper_version
        headers.update({'X-LaunchDarkly-Wrapper': config.wrapper_name + wrapper_version})
    return headers

def _http_factory(config):
    return HTTPFactory(_base_headers(config), config.http)

class HTTPFactory(object):
    def __init__(self, base_headers, http_config, override_read_timeout=None):
        self.__base_headers = base_headers
        self.__http_config = http_config
        self.__timeout = urllib3.Timeout(
            connect=http_config.connect_timeout,
            read=http_config.read_timeout if override_read_timeout is None else override_read_timeout
        )
    
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
            return urllib3.PoolManager(
                num_pools=num_pools,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs
                )
        else:
            return urllib3.ProxyManager(
                proxy_url,
                num_pools=num_pools,
                cert_reqs=cert_reqs,
                ca_certs = ca_certs
            )

def _get_proxy_url(target_base_uri):
    if target_base_uri is None:
        return None
    is_https = target_base_uri.startswith('https:')
    if is_https:
        return environ.get('https_proxy')
    return environ.get('http_proxy')
