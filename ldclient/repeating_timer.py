from threading import Event, Thread

class RepeatingTimer(Thread):
    def __init__(self, interval, callable):
        Thread.__init__(self)
        self.daemon = True
        self._interval = interval
        self._action = callable
        self._stop = Event()

    def run(self):
        while not self._stop.wait(self._interval):
            self._action()

    def stop(self):
        self._stop.set()
