from ldclient.interfaces import AsyncEventProcessor, EventProcessor


class NullEventProcessor(EventProcessor):
    def __init__(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def send_event(self, event):
        pass

    def flush(self):
        pass


class AsyncNullEventProcessor(AsyncEventProcessor):
    """Async no-op event processor. The async equivalent of
    :class:`NullEventProcessor`, so the async client can await ``stop()``
    uniformly whether events are enabled or not."""

    def start(self):
        pass

    async def stop(self):
        pass

    def send_event(self, event):
        pass

    def flush(self):
        pass

    async def flush_and_wait(self, timeout: float) -> bool:
        return True
