import logging
from ldclient.client import LDClient
from ldclient.twisted import TwistedConfig
from ldclient.twisted_sse import Event
import pytest
from testing.server_util import SSEServer, GenericServer
from testing.twisted_util import wait_until, is_equal

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
def server(request):
    server = GenericServer()

    def fin():
        server.shutdown()

    request.addfinalizer(fin)
    return server


@pytest.fixture()
def stream(request):
    server = SSEServer()

    def fin():
        server.shutdown()

    request.addfinalizer(fin)
    return server


@pytest.inlineCallbacks
def test_toggle(server):
    server.add_feature("foo", feature("foo", "jim")['foo'])
    client = LDClient("apikey", TwistedConfig(base_uri=server.url))
    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "jim"))


@pytest.inlineCallbacks
def test_sse_init(server, stream):
    stream.queue.put(Event(event="put", data=feature("foo", "jim")))
    client = LDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url))
    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "jim"))


@pytest.inlineCallbacks
def test_sse_reconnect(server, stream):
    server.post_events()
    stream.queue.put(Event(event="put", data=feature("foo", "on")))
    client = LDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url))
    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "on"))

    stream.stop()

    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "on"))

    stream.start()

    stream.queue.put(Event(event="put", data=feature("foo", "jim")))
    client = LDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url))
    yield wait_until(is_equal(lambda: client.toggle("foo", user('xyz'), "blah"), "jim"))


def feature(key, val):
    return {
        key: {"name": "Feature {}".format(key), "key": key, "kind": "flag", "salt": "Zm9v", "on": val,
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
