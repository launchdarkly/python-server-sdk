from queue import Queue

from ldclient.impl.listeners import Listeners


def test_notify_with_no_listeners_does_not_throw_exception():
    listeners = Listeners()
    listeners.notify("hi")


def test_notify_calls_listeners():
    q1 = Queue()
    q2 = Queue()
    listeners = Listeners()
    listeners.add(lambda v: q1.put(v))
    listeners.add(lambda v: q2.put(v))
    listeners.notify("hi")
    assert q1.get() == "hi"
    assert q2.get() == "hi"
    assert q1.empty() is True
    assert q2.empty() is True


def test_remove_listener():
    q1 = Queue()
    q2 = Queue()

    def put_into_q1(v):
        q1.put(v)

    def put_into_q2(v):
        q2.put(v)

    listeners = Listeners()
    listeners.add(put_into_q1)
    listeners.add(put_into_q2)
    listeners.remove(put_into_q1)
    listeners.remove(lambda v: print(v))  # removing nonexistent listener does not throw exception
    listeners.notify("hi")
    assert q1.empty() is True
    assert q2.get() == "hi"
    assert q2.empty() is True


def test_exception_from_listener_is_caught_and_other_listeners_are_still_called():
    def fail(v):
        raise Exception("deliberate error")

    q = Queue()
    listeners = Listeners()
    listeners.add(fail)
    listeners.add(lambda v: q.put(v))
    listeners.notify("hi")
    assert q.get() == "hi"
    assert q.empty() is True
