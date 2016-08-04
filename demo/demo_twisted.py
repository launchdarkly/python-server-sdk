from __future__ import print_function
from ldclient.twisted_impls import TwistedLDClient
from twisted.internet import task, defer


@defer.inlineCallbacks
def main(_):
    sdk_key = 'whatever'
    client = TwistedLDClient(sdk_key)
    user = {
        u'key': u'xyz',
        u'custom': {
            u'bizzle': u'def'
        }
    }
    val = yield client.variation('foo', user)
    yield client.flush()
    print("Value: {}".format(val))

if __name__ == '__main__':
    task.react(main)
