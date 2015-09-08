from __future__ import print_function
from ldclient.twisted import TwistedLDClient
from twisted.internet import task, defer

@defer.inlineCallbacks
def main(reactor):
    apiKey = 'whatever'
    client = TwistedLDClient(apiKey)
    user = {
        u'key': u'xyz',
        u'custom': {
            u'bizzle': u'def'
        }
    }
    val = yield client.toggle('foo', user)
    yield client.flush()
    print("Value: {}".format(val))

if __name__ == '__main__':
    task.react(main)