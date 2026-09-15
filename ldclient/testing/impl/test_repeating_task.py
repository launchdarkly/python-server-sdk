import logging
import time
from queue import Empty, Queue
from threading import Event

from ldclient.impl.delay import DelaySource
from ldclient.impl.repeating_task import RepeatingTask


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


class _RecordingDelay(DelaySource):
    """A delay source that records each read, so a test can see when the task
    asks for a wait rather than only what the action saw."""

    def __init__(self, seconds: float, events: list):
        self.seconds = seconds
        self._events = events

    @property
    def next_delay(self) -> float:
        self._events.append(('read', self.seconds))
        return self.seconds


def test_the_task_reads_the_delay_source_after_every_invocation():
    """One read per invocation, after it. A task that read the source once up
    front would show a read before the first invocation, and would never see
    the value the action set."""
    events: list = []
    delays = _RecordingDelay(0.01, events)

    def do_task():
        events.append('invoke')
        delays.seconds = 0.02

    task = RepeatingTask("ldclient.testing.recording-delay", delays, 0, do_task)
    try:
        task.start()
        deadline = time.time() + 2
        while events.count('invoke') < 3 and time.time() < deadline:
            time.sleep(0.005)
    finally:
        task.stop()

    # Reads and invocations alternate, starting with an invocation, and every
    # read sees 0.02 -- the initial 0.01 is never read.
    assert events[:5] == ['invoke', ('read', 0.02), 'invoke', ('read', 0.02), 'invoke']


def test_the_interval_starts_when_the_callback_returns():
    """A slow callback must not shorten its own wait: one invocation to the
    next is the interval plus however long the callback took."""
    work = 0.15
    interval = 0.15
    starts = Queue()

    def do_task():
        starts.put(time.time())
        time.sleep(work)

    task = RepeatingTask.at_interval("ldclient.testing.slow-callback", interval, 0, do_task)
    try:
        first = None
        task.start()
        first = starts.get(True, 2)
        second = starts.get(True, 2)
    finally:
        task.stop()

    # Measuring the interval from the start of the callback would give about
    # `interval`; measuring from its return gives interval + work. The 10%
    # slack is for scheduling noise, and leaves the two regimes far apart.
    assert (second - first) >= (interval + work) * 0.9


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
