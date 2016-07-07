import logging
from ldclient.client import Config, LDClient
from ldclient.twisted_sse import Event
import pytest
from testing.server_util import SSEServer, GenericServer
from testing.sync_util import wait_until

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


def test_toggle(server, stream):
    stream.queue.put(Event(event="put", data=feature("foo", True)))
    client = LDClient("apikey", Config(stream=True, base_uri=server.url, events_uri=server.url, stream_uri=stream.url))
    wait_until(lambda: client.toggle("foo", user('xyz'), False) is True)

# Doesn't seem to handle disconnects?
# def test_sse_reconnect(server, stream):
#     server.post_events()
#     stream.queue.put(Event(event="put", data=feature("foo", "on")))
#     client = LDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url))
#     wait_until(lambda: client.toggle("foo", user('xyz'), "blah") == "on")
#
#     stream.stop()
#
#     wait_until(lambda: client.toggle("foo", user('xyz'), "blah") == "on")
#
#     stream.start()
#
#     stream.queue.put(Event(event="put", data=feature("foo", "jim")))
#     client = LDClient("apikey", TwistedConfig(stream=True, base_uri=server.url, stream_uri=stream.url))
#     wait_until(lambda: client.toggle("foo", user('xyz'), "blah") == "jim")


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
