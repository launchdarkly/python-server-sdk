from ldclient.interfaces import EventProcessor, UpdateProcessor


class NullEventProcessor(EventProcessor):
    def __init__(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def is_alive(self):
        return False

    def send_event(self, event):
        pass

    def flush(self):
        pass


class NullUpdateProcessor(UpdateProcessor):
    def __init__(self, config, store, ready):
        self._ready = ready

    def start(self):
        self._ready.set()

    def stop(self):
        pass

    def is_alive(self):
        return False

    def initialized(self):
        return True
