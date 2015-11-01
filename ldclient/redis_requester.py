import json
from ldclient.expiringdict import ExpiringDict
from ldclient.interfaces import FeatureRequester
import redis


# noinspection PyUnusedLocal
def create_redis_ldd_requester(api_key, config, **kwargs):
    return RedisLDDRequester(config, **kwargs)


class ForgetfulDict(dict):

    def __setitem__(self, key, value):
        pass


class RedisLDDRequester(FeatureRequester):
    """
    Requests features from redis, usually stored via the LaunchDarkly Daemon (LDD).  Recommended to be combined
    with the ExpiringInMemoryFeatureStore
    """

    def __init__(self, config,
                 expiration=15,
                 redis_host='localhost',
                 redis_port=6379,
                 redis_prefix='launchdarkly'):
        """
        :type config: Config
        """
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._features_key = "{}:features".format(redis_prefix)
        self._cache = ForgetfulDict() if expiration == 0 else ExpiringDict(max_len=config.capacity,
                                                                           max_age_seconds=expiration)
        self._pool = None

    def _get_connection(self):
        if self._pool is None:
            self._pool = redis.ConnectionPool(
                host=self._redis_host, port=self._redis_port)
        return redis.Redis(connection_pool=self._pool)

    def get(self, key, callback):
        cached = self._cache.get(key)
        if cached is not None:
            return callback(cached)
        else:
            rd = self._get_connection()
            raw = rd.hget(self._features_key, key)
            if raw:
                val = json.loads(raw.decode('utf-8'))
            else:
                val = None
            self._cache[key] = val
            return callback(val)
