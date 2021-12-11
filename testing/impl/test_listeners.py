from ldclient.impl.listeners import Listeners

from queue import Queue

def test_notify_with_no_listeners_does_not_throw_exception():
    l = Listeners()
    l.notify("hi")

def test_notify_calls_listeners():
    q1 = Queue()
    q2 = Queue()
    l = Listeners()
    l.add(lambda v: q1.put(v))
    l.add(lambda v: q2.put(v))
    l.notify("hi")
    assert q1.get() == "hi"
    assert q2.get() == "hi"
    assert q1.empty() == True
    assert q2.empty() == True

def test_remove_listener():
    q1 = Queue()
    q2 = Queue()
    p1 = lambda v: q1.put(v)
    p2 = lambda v: q2.put(v)
    l = Listeners()
    l.add(p1)
    l.add(p2)
    l.remove(p1)
    l.remove(lambda v: print(v)) # removing nonexistent listener does not throw exception
    l.notify("hi")
    assert q1.empty() == True
    assert q2.get() == "hi"
    assert q2.empty() == True

def test_exception_from_listener_is_caught_and_other_listeners_are_still_called():
    def fail(v):
        raise Exception("deliberate error")
    q = Queue()
    l = Listeners()
    l.add(fail)
    l.add(lambda v: q.put(v))
    l.notify("hi")
    assert q.get() == "hi"
    assert q.empty() == True
