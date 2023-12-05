from ldclient.impl.events.types import EventInputEvaluation


class DebugEvent:
    __slots__ = ['original_input']

    def __init__(self, original_input: EventInputEvaluation):
        self.original_input = original_input
