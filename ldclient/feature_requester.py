"""
Default implementation of feature flag polling requests.
"""
# currently excluded from documentation - see docs/README.md

from collections import namedtuple
import json
import urllib3

from ldclient.impl.http import _http_factory
from ldclient.interfaces import FeatureRequester
from ldclient.util import UnsuccessfulResponseException
from ldclient.util import _headers
from ldclient.util import log
from ldclient.util import throw_if_unsuccessful_response
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


LATEST_ALL_URI = '/sdk/latest-all'


CacheEntry = namedtuple('CacheEntry', ['data', 'etag'])


class FeatureRequesterImpl(FeatureRequester):
    def __init__(self, config):
        self._cache = dict()
        self._http = _http_factory(config).create_pool_manager(1, config.base_uri)
        self._config = config
        self._poll_uri = config.base_uri + LATEST_ALL_URI

    def get_all_data(self):
        uri = self._poll_uri
        hdrs = _headers(self._config)
        cache_entry = self._cache.get(uri)
        if cache_entry is not None:
            hdrs['If-None-Match'] = cache_entry.etag
        r = self._http.request('GET', uri,
                               headers=hdrs,
                               timeout=urllib3.Timeout(connect=self._config.http.connect_timeout, read=self._config.http.read_timeout),
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
            if etag is not None:
                self._cache[uri] = CacheEntry(data=data, etag=etag)
        log.debug("%s response status:[%d] From cache? [%s] ETag:[%s]",
            uri, r.status, from_cache, etag)

        return {
            FEATURES: data['flags'],
            SEGMENTS: data['segments']
        }
