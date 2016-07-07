from functools import partial
import sys

from ldclient.redis_feature_store import RedisFeatureStore

sys.path.append("..")
sys.path.append("../testing")

from ldclient.util import Event
import logging
from ldclient.client import Config, LDClient
import pytest
from testing.server_util import SSEServer
from testing.sync_util import wait_until

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
def stream(request):
    server = SSEServer(port=8000)

    def fin():
        server.shutdown()

    request.addfinalizer(fin)
    return server


def test_sse_init(stream):
    stream.queue.put(Event(event="put", data=feature("foo", "jim")))
    client = LDClient("apikey", Config(use_ldd=True,
                                       feature_store=RedisFeatureStore(),
                                       events_enabled=False))
    wait_until(lambda: client.toggle(
        "foo", user('xyz'), "blah") == "jim", timeout=10)


def feature(key, val):
    return {
        key: {"name": "Feature {}".format(key), "key": key, "kind": "flag", "salt": "Zm9v", "on": True,
              "variations": [{"value": val, "weight": 100,
                              "targets": [{"attribute": "key", "op": "in", "values": []}],
                              "userTarget": {"attribute": "key", "op": "in", "values": []}},
                             {"value": False, "weight": 0,
                              "targets": [{"attribute": "key", "op": "in", "values": []}],
                              "userTarget": {"attribute": "key", "op": "in", "values": []}}],
              "commitDate": "2015-09-08T21:24:16.712Z",
              "creationDate": "2015-09-08T21:06:16.527Z", "version": 4}}


def user(name):
    return {
        u'key': name,
        u'custom': {
            u'bizzle': u'def'
        }
    }
