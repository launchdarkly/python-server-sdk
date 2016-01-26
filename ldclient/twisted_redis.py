import json
from ldclient.interfaces import StreamProcessor
from twisted.internet import task, defer, protocol, reactor
from txredis.client import RedisClient


# noinspection PyUnusedLocal
def create_redis_ldd_processor(api_key, config, store, **kwargs):
    return TwistedRedisLDDStreamProcessor(store, **kwargs)


class TwistedRedisLDDStreamProcessor(StreamProcessor):

    def __init__(self, store, update_delay=15, redis_host='localhost',
                 redis_port=6379,
                 redis_prefix='launchdarkly'):
        self._running = False

        if update_delay == 0:
            update_delay = .5
        self._update_delay = update_delay

        self._store = store
        """ :type: ldclient.interfaces.FeatureStore """

        self._features_key = "{}:features".format(redis_prefix)
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._looping_call = None

    def start(self):
        self._running = True
        self._looping_call = task.LoopingCall(self._refresh)
        self._looping_call.start(self._update_delay)

    def stop(self):
        self._looping_call.stop()

    def is_alive(self):
        return self._looping_call is not None and self._looping_call.running

    def _get_connection(self):
        client_creator = protocol.ClientCreator(reactor, RedisClient)
        return client_creator.connectTCP(self._redis_host, self._redis_port)

    @defer.inlineCallbacks
    def _refresh(self):
        redis = yield self._get_connection()
        """ :type: RedisClient """
        result = yield redis.hgetall(self._features_key)
        if result:
            data = {}
            for key, value in result.items():
                if value:
                    data[key] = json.loads(value.decode('utf-8'))
            self._store.init(data)
        else:
            self._store.init({})
