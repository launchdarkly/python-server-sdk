from __future__ import absolute_import

import json
from urlparse import urlparse

from twisted.internet import defer
from twisted.internet import protocol, reactor
from txredis.client import RedisClient

from ldclient.expiringdict import ExpiringDict
from ldclient.interfaces import FeatureStore
from ldclient.redis_feature_store import ForgetfulDict, INIT_KEY
from ldclient.util import log


class TwistedRedisFeatureStore(FeatureStore):
    def __init__(self,
                 url='redis://localhost:6379/0',
                 expiration=15,
                 capacity=1000,
                 redis_prefix='launchdarkly'):
        self._url = url
        parsed_url = urlparse(url)
        self._redis_host = parsed_url.hostname
        self._redis_port = parsed_url.port
        self._features_key = "{}:features".format(redis_prefix)
        self._cache = ForgetfulDict() if expiration == 0 else ExpiringDict(max_len=capacity,
                                                                           max_age_seconds=expiration)
        log.info("Created TwistedRedisFeatureStore with url: " + url + " using key: " + self._features_key)

    def _get_connection(self):
        client_creator = protocol.ClientCreator(reactor, RedisClient)
        return client_creator.connectTCP(self._redis_host, self._redis_port)

    def initialized(self):
        initialized = self._cache.get(INIT_KEY)
        if initialized:
            # reset ttl
            self._cache[INIT_KEY] = True
            return True

        @defer.inlineCallbacks
        def redis_initialized():
            r = yield self._get_connection()
            """ :type: RedisClient """
            i = yield r.exists(self._features_key)
            if i:
                # reset ttl
                self._cache[INIT_KEY] = True
            defer.returnValue(i)

        initialized = redis_initialized()
        return initialized

    @defer.inlineCallbacks
    def upsert(self, key, feature):
        r = yield self._get_connection()
        """ :type: RedisClient """
        r.watch(self._features_key)
        old = yield self.get(key)
        if old:
            if old['version'] >= feature['version']:
                r.unwatch()
                return

        feature_json = json.dumps(feature)
        r.hset(self._features_key, key, feature_json)
        self._cache[key] = feature
        r.unwatch()

    @defer.inlineCallbacks
    def all(self):
        r = yield self._get_connection()
        """ :type: RedisClient """
        all_features = yield r.hgetall(self._features_key)
        if all_features is None or all_features is "":
            log.warn("TwistedRedisFeatureStore: call to get all flags returned no results. Returning None.")
            defer.returnValue(None)

        results = {}
        for k, f_json in all_features.items() or {}:
            f = json.loads(f_json.decode('utf-8'))
            if 'deleted' in f and f['deleted'] is False:
                results[f['key']] = f
        defer.returnValue(results)

    @defer.inlineCallbacks
    def delete(self, key, version):
        r = yield self._get_connection()
        """ :type: RedisClient """
        r.watch(self._features_key)
        f_json = yield r.hget(self._features_key, key)
        if f_json:
            f = json.loads(f_json.decode('utf-8'))
            if f is not None and f['version'] < version:
                f['deleted'] = True
                f['version'] = version
            elif f is None:
                f = {'deleted': True, 'version': version}
            f_json = json.dumps(f)
            r.hset(self._features_key, key, f_json)
            self._cache[key] = f
        r.unwatch()

    @defer.inlineCallbacks
    def init(self, features):
        r = yield self._get_connection()
        """ :type: RedisClient """

        r.multi()
        r.delete(self._features_key)
        self._cache.clear()

        for k, f in features.items():
            f_json = json.dumps(f)
            r.hset(self._features_key, k, f_json)
            self._cache[k] = f
        r.execute()
        log.info("Initialized TwistedRedisFeatureStore with " + str(len(features)) + " feature flags")

    @defer.inlineCallbacks
    def get(self, key):
        cached = self._cache.get(key)
        if cached is not None:
            defer.returnValue(cached)
        else:
            r = yield self._get_connection()
            """ :type: RedisClient """
            f_json = yield r.hget(self._features_key, key)
            if f_json is None or f_json is "":
                log.warn(
                    "TwistedRedisFeatureStore: feature flag with key: " + key + " not found in Redis. Returning None.")
                defer.returnValue(None)

            f = json.loads(f_json.decode('utf-8'))
            if f.get('deleted', False) is True:
                log.warn("TwistedRedisFeatureStore: get returned deleted flag from Redis. Returning None.")
                defer.returnValue(None)

            self._cache[key] = f
            defer.returnValue(f)
