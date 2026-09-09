import logging
import time
from queue import Empty, Queue
from threading import Event

from ldclient.impl.repeating_task import DelaySource, FixedDelay, RepeatingTask


def test_task_does_not_start_when_created():
    signal = Event()
    task = RepeatingTask.at_interval("ldclient.testing.set-signal", 0.01, 0, lambda: signal.set())
    try:
        signal_was_set = signal.wait(0.1)
        assert signal_was_set is False
    finally:
        task.stop()


def test_a_second_start_logs_and_does_not_raise(caplog):
    """A raise here can surface out of a caller that is documented as safe to
    call more than once, such as AsyncLDClient.start()."""
    caplog.set_level(logging.INFO)
    queue = Queue()
    task = RepeatingTask.at_interval("ldclient.testing.enqueue-time", 0.01, 0, lambda: queue.put(time.time()))
    try:
        task.start()
        thread = task._RepeatingTask__thread

        task.start()

        assert task._RepeatingTask__thread is thread
        assert queue.get(True, 1) is not None  # still running
    finally:
        task.stop()

    assert any(
        r.getMessage() == "Task ldclient.testing.enqueue-time has already been started; ignoring"
        for r in caplog.records
    )


def test_a_start_after_stop_does_not_resume_the_task():
    counter = 0

    def do_task():
        nonlocal counter
        counter += 1

    task = RepeatingTask.at_interval("ldclient.testing.task-runner", 0.01, 0, do_task)
    task.stop()
    task.start()
    time.sleep(0.1)

    assert counter == 0


def test_task_executes_until_stopped():
    queue = Queue()
    task = RepeatingTask.at_interval("ldclient.testing.enqueue-time", 0.1, 0, lambda: queue.put(time.time()))
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


class _MutableDelay(DelaySource):
    """A delay source a test can move between invocations."""

    def __init__(self, seconds: float):
        self.seconds = seconds

    @property
    def next_delay(self) -> float:
        return self.seconds


def test_fixed_delay_always_gives_the_same_wait():
    delays = FixedDelay(2.5)
    assert delays.next_delay == 2.5
    assert delays.next_delay == 2.5


def test_the_task_reads_the_delay_source_after_every_invocation():
    """A value the action decides takes effect on the next wait."""
    reads = Queue()
    delays = _MutableDelay(0.01)

    def do_task():
        reads.put(delays.seconds)
        delays.seconds = 0.02  # what the next wait must use

    task = RepeatingTask("ldclient.testing.mutable-delay", delays, 0, do_task)
    try:
        task.start()
        assert reads.get(True, 1) == 0.01
        assert reads.get(True, 1) == 0.02
        assert reads.get(True, 1) == 0.02
    finally:
        task.stop()


def test_whatever_the_action_returns_is_ignored():
    """Guards big-segment polling, whose action returns a status object."""
    calls = Queue()

    def do_task():
        calls.put(time.time())
        return object()  # not a number, and not for the task to interpret

    task = RepeatingTask.at_interval("ldclient.testing.returns-a-value", 0.01, 0, do_task)
    try:
        task.start()
        for _ in range(3):
            assert calls.get(True, 1) is not None
    finally:
        task.stop()


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

    task = RepeatingTask.at_interval("ldclient.testing.task-runner", 0.01, 0, do_task)
    try:
        task.start()
        assert stopped.wait(0.1) is True
        assert counter == 2
        time.sleep(0.1)
        assert counter == 2
    finally:
        task.stop()
