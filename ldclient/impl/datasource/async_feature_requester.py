"""
Default implementation of feature flag polling requests.
"""

import json
from collections import namedtuple
from typing import Optional
from urllib import parse

from ldclient.impl.aio.transport import AsyncHTTPTransport
from ldclient.impl.util import _headers, log, throw_if_unsuccessful_response
from ldclient.interfaces import FeatureRequester
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

FDV1_POLLING_ENDPOINT = '/sdk/latest-all'


CacheEntry = namedtuple('CacheEntry', ['data', 'etag'])


class AsyncFeatureRequesterImpl(FeatureRequester):
    def __init__(self, config, transport: Optional[AsyncHTTPTransport] = None):
        self._cache: dict = dict()
        self._transport = transport if transport is not None else AsyncHTTPTransport(config)
        self._config = config
        self._poll_uri = config.base_uri + FDV1_POLLING_ENDPOINT
        if config.payload_filter_key is not None:
            self._poll_uri += '?%s' % parse.urlencode({'filter': config.payload_filter_key})

    async def get_all_data(self):
        uri = self._poll_uri
        hdrs = _headers(self._config)
        cache_entry = self._cache.get(uri)
        hdrs['Accept-Encoding'] = 'gzip'
        if cache_entry is not None:
            hdrs['If-None-Match'] = cache_entry.etag
        r = await self._transport.request('GET', uri, headers=hdrs)
        throw_if_unsuccessful_response(r)
        if r.status == 304 and cache_entry is not None:
            data = cache_entry.data
            etag = cache_entry.etag
            from_cache = True
        else:
            data = json.loads(r.body)
            etag = r.headers.get('ETag')
            from_cache = False
            if etag is not None:
                self._cache[uri] = CacheEntry(data=data, etag=etag)
        log.debug("%s response status:[%d] From cache? [%s] ETag:[%s]", uri, r.status, from_cache, etag)

        return {FEATURES: data['flags'], SEGMENTS: data['segments']}


# Backwards-compatible alias for the previous class name
AsyncFeatureRequester = AsyncFeatureRequesterImpl
