from threading import Event, Lock, Thread

# noinspection PyBroadException
try:
    import queue
except:
    # noinspection PyUnresolvedReferences,PyPep8Naming
    import Queue as queue

from ldclient.util import log

"""
A simple fixed-size thread pool that rejects jobs when its limit is reached.
"""
class FixedThreadPool(object):
    def __init__(self, size, name):
        self._size = size
        self._lock = Lock()
        self._busy_count = 0
        self._event = Event()
        self._job_queue = queue.Queue()
        for i in range(0, size):
            thread = Thread(target = self._run_worker)
            thread.name = "%s.%d" % (name, i + 1)
            thread.daemon = True
            thread.start()
    
    """
    Schedules a job for execution if there is an available worker thread, and returns
    true if successful; returns false if all threads are busy.
    """
    def execute(self, jobFn):
        with self._lock:
            if self._busy_count >= self._size:
                return False
            self._busy_count = self._busy_count + 1
        self._job_queue.put(jobFn)
        return True
    
    """
    Waits until all currently busy worker threads have completed their jobs.
    """
    def wait(self):
        while True:
            with self._lock:
                if self._busy_count == 0:
                    return
                self._event.clear()
            self._event.wait()
    
    """
    Tells all the worker threads to terminate once all active jobs have completed.
    """
    def stop(self):
        for i in range(0, self._size):
            self._job_queue.put('stop')
    
    def _run_worker(self):
        while True:
            item = self._job_queue.get(block = True)
            if item is 'stop':
                return
            try:
                item()
            except Exception:
                log.warning('Unhandled exception in worker thread', exc_info=True)
            with self._lock:
                self._busy_count = self._busy_count - 1
                self._event.set()
