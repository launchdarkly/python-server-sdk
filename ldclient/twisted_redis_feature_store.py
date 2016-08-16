from __future__ import absolute_import

import json
import urlparse

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
        parsed_url = urlparse.urlparse(url)
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

    def upsert(self, key, feature):
        raise NotImplementedError()

    def all(self, callback):
        @defer.inlineCallbacks
        def redis_get_all():
            r = None
            try:
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
            except Exception as e:
                log.error("Could not connect to Redis using url: " + self._url + " with error message: " + e.message)
                defer.returnValue(None)
            finally:
                if r:
                    r.quit()
            defer.returnValue(None)

        all_flags = redis_get_all()
        all_flags.addBoth(callback)
        return all_flags

    def delete(self, key, version):
        raise NotImplementedError()

    def init(self, features):
        raise NotImplementedError()

    def get(self, key, callback):
        @defer.inlineCallbacks
        def redis_get():
            r = None
            try:
                r = yield self._get_connection()
                """ :type: RedisClient """
                get_result = yield r.hget(self._features_key, key)
                if not get_result:
                    log.warn("Didn't get response from redis for key: " + key + " Returning None.")
                    defer.returnValue(None)
                f_json = get_result.get(key)
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
            except Exception as e:
                log.error("Could not connect to Redis using url: " + self._url + " with error message: " + e.message)
                defer.returnValue(None)
            finally:
                if r:
                    r.quit()
            defer.returnValue(None)

        cached = self._cache.get(key)
        if cached is not None:
            # reset ttl
            self._cache[key] = cached
            return callback(cached)

        f = redis_get()
        f.addBoth(callback)
        return f
