from ldclient.context import Context


class IndexEvent:
    __slots__ = ['timestamp', 'context']

    def __init__(self, timestamp: int, context: Context):
        self.timestamp = timestamp
        self.context = context
