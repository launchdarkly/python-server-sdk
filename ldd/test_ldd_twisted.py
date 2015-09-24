import sys
sys.path.append("..")
sys.path.append("../testing")

from ldclient.noop import NoOpFeatureRequester
from ldclient import TwistedConfig
from ldclient.twisted_redis import create_redis_ldd_processor
from testing.twisted_util import is_equal, wait_until
from ldclient.util import Event
import logging
from ldclient.client import LDClient
import pytest
from testing.server_util import SSEServer

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
def stream(request):
    server = SSEServer(port=8000)

    def fin():
        server.shutdown()

    request.addfinalizer(fin)
    return server


@pytest.inlineCallbacks
def test_sse_init(stream):
    stream.queue.put(Event(event="put", data=feature("foo", "jim")))
    client = LDClient("apikey", TwistedConfig(stream=True, stream_processor_class=create_redis_ldd_processor,
                                              feature_requester_class=NoOpFeatureRequester,
                                              events=False))
    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "jim"))


def feature(key, val):
    return {
        key: {"name": "Feature {}".format(key), "key": key, "kind": "flag", "salt": "Zm9v", "on": True,
              "variations": [{"value": val, "weight": 100,
                              "targets": [{"attribute": "key", "op": "in", "values": []}],
                              "userTarget": {"attribute": "key", "op": "in", "values": []}},
                             {"value": False, "weight": 0,
                              "targets": [{"attribute": "key", "olikep": "in", "values": []}],
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
