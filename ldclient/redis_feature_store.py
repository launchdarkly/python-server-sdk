import json
from pprint import pprint

import redis

from ldclient import log
from ldclient.expiringdict import ExpiringDict
from ldclient.interfaces import FeatureStore

INIT_KEY = "$initialized$"


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

        self._features_key = "{}:features".format(prefix)
        self._cache = ForgetfulDict() if expiration == 0 else ExpiringDict(max_len=capacity,
                                                                           max_age_seconds=expiration)
        self._pool = redis.ConnectionPool.from_url(url=url, max_connections=max_connections)
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

    def all(self, callback):
        r = redis.Redis(connection_pool=self._pool)
        all_features = r.hgetall(self._features_key)
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
        f = self._cache.get(key)
        if f is not None:
            # reset ttl
            self._cache[key] = f
            if f.get('deleted', False) is True:
                log.warn("RedisFeatureStore: get returned deleted flag from in-memory cache. Returning None.")
                return callback(None)
            return callback(f)

        r = redis.Redis(connection_pool=self._pool)
        f_json = r.hget(self._features_key, key)
        if f_json is None or f_json is "":
            log.warn("RedisFeatureStore: feature flag with key: " + key + " not found in Redis. Returning None.")
            return callback(None)

        f = json.loads(f_json.decode('utf-8'))
        if f.get('deleted', False) is True:
            log.warn("RedisFeatureStore: get returned deleted flag from Redis. Returning None.")
            return callback(None)
        self._cache[key] = f
        return callback(f)

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
        initialized = self._cache.get(INIT_KEY)
        if initialized:
            # reset ttl
            self._cache[INIT_KEY] = True
            return True

        r = redis.Redis(connection_pool=self._pool)
        if r.exists(self._features_key):
            self._cache[INIT_KEY] = True
            return True
        return False

    def upsert(self, key, feature):
        r = redis.Redis(connection_pool=self._pool)
        r.watch(self._features_key)
        old = self.get(key)
        if old:
            if old['version'] >= feature['version']:
                r.unwatch()
                return

        feature_json = json.dumps(feature)
        r.hset(self._features_key, key, feature_json)
        self._cache[key] = feature
        r.unwatch()
