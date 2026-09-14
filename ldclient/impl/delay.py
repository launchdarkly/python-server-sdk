"""
The wait a repeating task takes between invocations. Both schedulers read it,
so it belongs to neither.
"""

# currently excluded from documentation - see docs/README.md

from typing import Protocol


class DelaySource(Protocol):
    """Supplies the wait before a repeating task's next invocation."""

    @property
    def next_delay(self) -> float:
        """The seconds to wait before the next invocation."""
        ...


class FixedDelay(DelaySource):
    """A :class:`DelaySource` that always gives the same wait."""

    def __init__(self, seconds: float):
        self.__seconds = seconds

    @property
    def next_delay(self) -> float:
        return self.__seconds
