"""
Default implementation of feature flag polling requests.
"""

import json
from collections import namedtuple
from typing import Optional
from urllib import parse

from ldclient.impl.aio.transport import AsyncHTTPTransport
from ldclient.impl.datasource.datasource_common import FDV1_POLLING_ENDPOINT
from ldclient.impl.util import _headers, log, throw_if_unsuccessful_response
from ldclient.interfaces import AsyncFeatureRequester
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

CacheEntry = namedtuple('CacheEntry', ['data', 'etag'])


class AsyncFeatureRequesterImpl(AsyncFeatureRequester):
    def __init__(self, config, transport: Optional[AsyncHTTPTransport] = None):
        self._cache: dict = dict()
        # Only close the transport on shutdown if we created it; an injected
        # transport is owned by the caller.
        self._owns_transport = transport is None
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

    async def close(self):
        if self._owns_transport:
            await self._transport.close()
