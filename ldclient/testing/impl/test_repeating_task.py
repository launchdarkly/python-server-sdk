import time
from queue import Empty, Queue
from threading import Event

from ldclient.impl.repeating_task import RepeatingTask


def test_task_does_not_start_when_created():
    signal = Event()
    task = RepeatingTask("ldclient.testing.set-signal", 0.01, 0, lambda: signal.set())
    try:
        signal_was_set = signal.wait(0.1)
        assert signal_was_set is False
    finally:
        task.stop()


def test_task_executes_until_stopped():
    queue = Queue()
    task = RepeatingTask("ldclient.testing.enqueue-time", 0.1, 0, lambda: queue.put(time.time()))
    try:
        last = None
        task.start()
        for _ in range(3):
            t = queue.get(True, 1)
            if last is not None:
                assert (time.time() - last) >= 0.05
            last = t
    finally:
        task.stop()
    stopped_time = time.time()
    no_more_items = False
    for _ in range(2):
        try:
            t = queue.get(False)
            assert t <= stopped_time
        except Empty:
            no_more_items = True
    assert no_more_items is True


def test_task_can_be_stopped_from_within_the_task():
    counter = 0
    stopped = Event()
    task = None

    def do_task():
        nonlocal counter
        counter += 1
        if counter >= 2:
            task.stop()
            stopped.set()

    task = RepeatingTask("ldclient.testing.task-runner", 0.01, 0, do_task)
    try:
        task.start()
        assert stopped.wait(0.1) is True
        assert counter == 2
        time.sleep(0.1)
        assert counter == 2
    finally:
        task.stop()
