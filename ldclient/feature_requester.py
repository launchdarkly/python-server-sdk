from __future__ import absolute_import

import requests
from cachecontrol import CacheControl

from ldclient.interfaces import FeatureRequester
from ldclient.util import _headers
from ldclient.util import log


class FeatureRequesterImpl(FeatureRequester):
    def __init__(self, sdk_key, config):
        self._sdk_key = sdk_key
        self._session = CacheControl(requests.Session())
        self._config = config

    def get_all(self):
        hdrs = _headers(self._sdk_key)
        uri = self._config.get_latest_features_uri
        log.debug("Getting all flags using uri: " + uri)
        r = self._session.get(uri, headers=hdrs, timeout=(
            self._config.connect_timeout, self._config.read_timeout))
        r.raise_for_status()
        features = r.json()
        return features

    def get_one(self, key):
        hdrs = _headers(self._sdk_key)
        uri = self._config.get_latest_features_uri + '/' + key
        log.debug("Getting one feature flag using uri: " + uri)
        r = self._session.get(uri,
                              headers=hdrs,
                              timeout=(self._config.connect_timeout,
                                       self._config.read_timeout))
        r.raise_for_status()
        feature = r.json()
        return feature
