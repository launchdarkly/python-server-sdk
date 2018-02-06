from __future__ import absolute_import

import requests
from cachecontrol import CacheControl

from ldclient.interfaces import FeatureRequester
from ldclient.util import _headers
from ldclient.util import log
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


LATEST_ALL_URI = '/sdk/latest-all'


class FeatureRequesterImpl(FeatureRequester):
    def __init__(self, config):
        self._session_cache = CacheControl(requests.Session())
        self._session_no_cache = requests.Session()
        self._config = config

    def get_all_data(self):
        hdrs = _headers(self._config.sdk_key)
        uri = self._config.base_uri + LATEST_ALL_URI
        r = self._session_cache.get(uri,
                                    headers=hdrs,
                                    timeout=(
                                        self._config.connect_timeout,
                                        self._config.read_timeout))
        r.raise_for_status()
        all_data = r.json()
        log.debug("Get All flags response status:[%d] From cache?[%s] ETag:[%s]",
                  r.status_code, r.from_cache, r.headers.get('ETag'))
        return {
            FEATURES: all_data['flags'],
            SEGMENTS: all_data['segments']
        }

    def get_one(self, kind, key):
        hdrs = _headers(self._config.sdk_key)
        path = kind.request_api_path + '/' + key
        uri = config.base_uri + path
        log.debug("Getting %s from %s using uri: %s", key, kind['namespace'], uri)
        r = self._session_no_cache.get(uri,
                                       headers=hdrs,
                                       timeout=(
                                           self._config.connect_timeout,
                                           self._config.read_timeout))
        r.raise_for_status()
        obj = r.json()
        log.debug("%s response status:[%d] key:[%s] version:[%d]",
                  path, r.status_code, key, segment.get("version"))
        return obj
