from threading import Event, Thread

class RepeatingTimer(object):
    def __init__(self, interval, callable):
        self._interval = interval
        self._action = callable
        self._stop = Event()
        self._thread = Thread(target=self._run)
        self._thread.daemon = True

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.wait(self._interval):
            self._action()
