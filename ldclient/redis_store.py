import json
from pprint import pprint

import redis

from ldclient import log
from ldclient.expiringdict import ExpiringDict
from ldclient.interfaces import FeatureStore
from ldclient.memoized_value import MemoizedValue


class ForgetfulDict(dict):
    def __setitem__(self, key, value):
        pass


class RedisFeatureStore(FeatureStore):
    def __init__(self,
                 url='redis://localhost:6379/0',
                 prefix='launchdarkly',
                 max_connections=16,
                 expiration=15,
                 capacity=1000):

        self._features_key = "{0}:features".format(prefix)
        self._cache = ForgetfulDict() if expiration == 0 else ExpiringDict(max_len=capacity,
                                                                           max_age_seconds=expiration)
        self._pool = redis.ConnectionPool.from_url(url=url, max_connections=max_connections)
        self._inited = MemoizedValue(lambda: self._query_init())
        log.info("Started RedisFeatureStore connected to URL: " + url + " using prefix: " + prefix)

    def init(self, features):
        pipe = redis.Redis(connection_pool=self._pool).pipeline()
        pipe.delete(self._features_key)

        self._cache.clear()

        for k, f in features.items():
            f_json = json.dumps(f)
            pipe.hset(self._features_key, k, f_json)
            self._cache[k] = f
        pipe.execute()
        log.info("Initialized RedisFeatureStore with " + str(len(features)) + " feature flags")
        self._inited.set(True)

    def all(self, callback):
        r = redis.Redis(connection_pool=self._pool)
        try:
            all_features = r.hgetall(self._features_key)
        except BaseException as e:
            log.error("RedisFeatureStore: Could not retrieve all flags from Redis with error: "
                      + e.message + " Returning None")
            return callback(None)

        if all_features is None or all_features is "":
            log.warn("RedisFeatureStore: call to get all flags returned no results. Returning None.")
            return callback(None)

        results = {}
        for k, f_json in all_features.items() or {}:
            f = json.loads(f_json.decode('utf-8'))
            if 'deleted' in f and f['deleted'] is False:
                results[f['key']] = f
        return callback(results)

    def get(self, key, callback=lambda x: x):
        f = self._get_even_if_deleted(key)
        if f is not None:
            if f.get('deleted', False) is True:
                log.debug("RedisFeatureStore: get returned deleted flag from Redis. Returning None.")
                return callback(None)
        return callback(f)

    def _get_even_if_deleted(self, key):
        f = self._cache.get(key)
        if f is not None:
            # reset ttl
            self._cache[key] = f
            return f

        try:
            r = redis.Redis(connection_pool=self._pool)
            f_json = r.hget(self._features_key, key)
        except BaseException as e:
            log.error("RedisFeatureStore: Could not retrieve flag from redis with error: " + e.message
                      + ". Returning None for key: " + key)
            return None

        if f_json is None or f_json is "":
            log.debug("RedisFeatureStore: feature flag with key: " + key + " not found in Redis. Returning None.")
            return None

        f = json.loads(f_json.decode('utf-8'))
        self._cache[key] = f
        return f

    def delete(self, key, version):
        r = redis.Redis(connection_pool=self._pool)
        r.watch(self._features_key)
        f_json = r.hget(self._features_key, key)
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

    @property
    def initialized(self):
        return self._inited.get()

    def _query_init(self):
        r = redis.Redis(connection_pool=self._pool)
        return r.exists(self._features_key)

    def upsert(self, key, feature):
        r = redis.Redis(connection_pool=self._pool)
        r.watch(self._features_key)
        old = self._get_even_if_deleted(key)
        if old:
            if old['version'] >= feature['version']:
                r.unwatch()
                return

        feature_json = json.dumps(feature)
        r.hset(self._features_key, key, feature_json)
        self._cache[key] = feature
        r.unwatch()
