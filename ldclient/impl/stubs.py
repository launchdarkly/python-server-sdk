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


class AsyncNullEventProcessor:
    """Async no-op event processor. The async equivalent of
    :class:`NullEventProcessor`, so the async client can await ``stop()``
    uniformly whether events are enabled or not."""

    def start(self):
        pass

    async def stop(self):
        pass

    def is_alive(self):
        return False

    def send_event(self, event):
        pass

    def flush(self):
        pass


class AsyncNullUpdateProcessor:
    """Async no-op update processor. The async equivalent of
    :class:`NullUpdateProcessor`, used by async FDv1 when offline or in LDD
    mode so the data system can await ``stop()`` uniformly."""

    def __init__(self, config, store, ready):
        self._ready = ready

    def start(self):
        self._ready.set()

    async def stop(self):
        pass

    def is_alive(self):
        return False

    def initialized(self):
        return True
