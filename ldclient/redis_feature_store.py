import json

import redis

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

    def init(self, features):
        pipe = redis.Redis(connection_pool=self._pool).pipeline()
        pipe.delete(self._features_key)

        self._cache.clear()

        for k, f in features.items():
            f_json = json.dumps(f)
            pipe.hset(self._features_key, k, f_json)
            self._cache[k] = f
        pipe.execute()

    def all(self):
        r = redis.Redis(connection_pool=self._pool)
        all_features = r.hgetall(self._features_key)
        results = {}
        for f_json in all_features:
            f = json.loads(f_json.decode('utf-8'))
            if 'deleted' in f and f['deleted'] is False:
                results[f['key']] = f
        return results

    def get(self, key):
        f = self._cache.get(key)
        if f:
            # reset ttl
            self._cache[key] = f
            if 'deleted' in f and f['deleted']:
                return None
            return f

        r = redis.Redis(connection_pool=self._pool)
        f_json = r.hget(self._features_key, key)
        if f_json:
            f = json.loads(f_json.decode('utf-8'))
            if f:
                if 'deleted' in f and f['deleted']:
                    return None
            self._cache[key] = f
            return f

        return None

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
