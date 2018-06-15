from __future__ import absolute_import

from collections import namedtuple
import json
import urllib3

from ldclient.interfaces import FeatureRequester
from ldclient.util import UnsuccessfulResponseException
from ldclient.util import _headers
from ldclient.util import create_http_pool_manager
from ldclient.util import log
from ldclient.util import throw_if_unsuccessful_response
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


LATEST_ALL_URI = '/sdk/latest-all'


CacheEntry = namedtuple('CacheEntry', ['data', 'etag'])


class FeatureRequesterImpl(FeatureRequester):
    def __init__(self, config):
        self._cache = dict()
        self._http = create_http_pool_manager(num_pools=1, verify_ssl=config.verify_ssl)
        self._config = config

    def get_all_data(self):
        all_data = self._do_request(self._config.base_uri + LATEST_ALL_URI, True)
        return {
            FEATURES: all_data['flags'],
            SEGMENTS: all_data['segments']
        }

    def get_one(self, kind, key):
        return self._do_request(kind.request_api_path + '/' + key, False)

    def _do_request(self, uri, allow_cache):
        hdrs = _headers(self._config.sdk_key)
        if allow_cache:
            cache_entry = self._cache.get(uri)
            if cache_entry is not None:
                hdrs['If-None-Match'] = cache_entry.etag
        r = self._http.request('GET', uri,
                               headers=hdrs,
                               timeout=urllib3.Timeout(connect=self._config.connect_timeout, read=self._config.read_timeout),
                               retries=1)
        throw_if_unsuccessful_response(r)
        if r.status == 304 and cache_entry is not None:
            data = cache_entry.data
            etag = cache_entry.etag
            from_cache = True
        else:
            data = json.loads(r.data.decode('UTF-8'))
            etag = r.getheader('ETag')
            from_cache = False
            if allow_cache and etag is not None:
                self._cache[uri] = CacheEntry(data=data, etag=etag)
        log.debug("%s response status:[%d] From cache? [%s] ETag:[%s]",
            uri, r.status, from_cache, etag)
        return data
