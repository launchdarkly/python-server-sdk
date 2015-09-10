import logging
from ldclient.twisted import TwistedLDClient, TwistedConfig
from ldclient.twisted_sse import Event
import pytest
from testing.sse_util import wait_until, SSEServer, GenericServer

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
def test_sse_init(server, stream):
    stream.queue.put(Event(event="put/features", data=feature("foo", True)))
    client = TwistedLDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url + "/"))
    yield wait_until(lambda: client.toggle("foo", user('xyz'), False))


@pytest.skip
@pytest.inlineCallbacks
def test_sse_reconnect(server):
    server.post_events()
    with SSEServer() as stream:
        stream.queue.put(Event(event="put/features", data=feature("foo", True)))
        client = TwistedLDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url + "/all"))
        yield wait_until(lambda: client.toggle("foo", user('xyz'), False))

    yield wait_until(lambda: client.toggle("foo", user, False))

    with SSEServer() as stream:
        stream.queue.put(Event(event="put/features", data=feature("foo", False)))
        client = TwistedLDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url + "/all"))
        yield wait_until(lambda: not client.toggle("foo", user('xyz'), True))


def feature(key, val):
    return {
        key: {"name": "Feature {}".format(key), "key": key, "kind": "flag", "salt": "Zm9v", "on": val,
                "variations": [{"value": True, "weight": 100,
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
