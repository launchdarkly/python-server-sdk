import time

from twisted.internet import defer, reactor


@defer.inlineCallbacks
def wait_until(condition, timeout=5):
    end_time = time.time() + timeout

    while True:
        result = yield defer.maybeDeferred(condition)
        if result:
            defer.returnValue(condition)
        elif time.time() > end_time:
            raise Exception("Timeout waiting for {}".format(condition.__name__))  # pragma: no cover
        else:
            d = defer.Deferred()
            reactor.callLater(.1, d.callback, None)
            yield d


def is_equal(f, val):
    @defer.inlineCallbacks
    def is_equal_eval():
        result = yield defer.maybeDeferred(f)
        defer.returnValue(result == val)

    return is_equal_eval