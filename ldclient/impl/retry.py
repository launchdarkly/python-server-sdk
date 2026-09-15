"""
Computes how long to wait before a failed operation is tried again.

Each failure falls into one of two classes. A ``NORMAL`` failure is one the
service is expected to recover from soon, so the wait stays short. An
``UNEXPECTED`` failure points to a problem that a person has to fix, such as a
rejected SDK key, so the wait becomes much longer. Only an HTTP status can be
``UNEXPECTED``; every network and TLS failure is ``NORMAL``. Neither class ever
tells the caller to give up. There is always a next attempt.

The wait doubles after each failure, up to a ceiling. A random jitter is then
subtracted, so that many callers do not all try again at the same moment. The
wait never falls below the caller's operating cadence.

:class:`RetryState` holds the state for one caller. Use :func:`for_streaming`
or :func:`for_polling` to build one with the right parameters and reset policy.
"""

# currently excluded from documentation - see docs/README.md

import math
import random
import time
from enum import Enum
from typing import Optional, Protocol

from ldclient.impl.util import log

# The delay bounds of the extended regime, in seconds. A component enters the
# extended regime after an unexpected failure.
EXTENDED_INITIAL_DELAY = 5 * 60
EXTENDED_MAX_DELAY = 60 * 60

# The delay bounds of the normal regime for streaming, in seconds. The initial
# delay is configurable as ``initial_reconnect_delay``.
STREAMING_MAX_DELAY = 30

# The documented defaults, in seconds. Each stands in for a configured value
# that is not a positive, finite number.
DEFAULT_INITIAL_RECONNECT_DELAY = 1
DEFAULT_POLL_INTERVAL = 30

# How long streaming must operate without a failure before its retry state
# resets, in seconds.
STREAMING_RESET_INTERVAL = 60

# How many polls in a row must succeed before polling's retry state resets.
POLLING_RESET_SUCCESSES = 2

# HTTP statuses in the 4xx range that are still normal failures. Every other
# 4xx is unexpected.
_NORMAL_4XX_STATUSES = frozenset([400, 408, 429])

# An upper bound on the backoff exponent, so a long outage cannot overflow the
# delay computation. Any real ceiling is reached long before this.
_MAX_BACKOFF_EXPONENT = 30


class FailureKind(Enum):
    """How a failure is classified, which decides how long the next wait is."""

    NORMAL = 'normal'
    """A failure the service is expected to recover from without help."""

    UNEXPECTED = 'unexpected'
    """A failure that suggests a problem a person has to fix. The component
    keeps retrying, but much less often."""


def classify_http_status(status: int) -> FailureKind:
    """
    Classifies an HTTP status.

    ``400``, ``408`` and ``429`` are normal, as is any ``5xx``. Every other
    ``4xx`` -- including ``401`` and ``403`` -- is unexpected.
    """
    if 400 <= status < 500 and status not in _NORMAL_4XX_STATUSES:
        return FailureKind.UNEXPECTED
    return FailureKind.NORMAL


class ResetPolicy(Protocol):
    """Decides when a component has operated well enough for long enough that
    its retry state should reset. This is the only behavioral difference
    between streaming and polling."""

    def note_healthy(self) -> None:
        """Records that the component is operating normally."""
        ...

    def note_failure(self) -> None:
        """Records a failure, which ends any healthy stretch in progress."""
        ...

    def is_satisfied(self) -> bool:
        """Reports whether the reset condition is met."""
        ...


class AfterHealthyFor(ResetPolicy):
    """Resets once the component has operated without failing for
    ``seconds``. This is the streaming policy."""

    def __init__(self, seconds: float):
        self._healthy_seconds = seconds
        self._healthy_since: Optional[float] = None

    def note_healthy(self) -> None:
        """Records the monotonic time the component became healthy. Calling
        this again while it is still healthy does not move that time."""
        if self._healthy_since is None:
            self._healthy_since = time.monotonic()

    def note_failure(self) -> None:
        self._healthy_since = None

    def is_satisfied(self) -> bool:
        if self._healthy_since is None:
            return False
        return time.monotonic() - self._healthy_since >= self._healthy_seconds

    @property
    def healthy_since(self) -> Optional[float]:
        """When the current healthy stretch began, or None if the component is
        not currently healthy."""
        return self._healthy_since


class AfterConsecutiveSuccesses(ResetPolicy):
    """Resets once ``count`` operations in a row have succeeded. This is the
    polling policy."""

    def __init__(self, count: int):
        self._count = count
        self._successes = 0

    def note_healthy(self) -> None:
        self._successes += 1

    def note_failure(self) -> None:
        self._successes = 0

    def is_satisfied(self) -> bool:
        return self._successes >= self._count

    @property
    def successes(self) -> int:
        """How many operations have succeeded in a row."""
        return self._successes


class RetryState:
    """
    Tracks how long a data source should wait before its next attempt.

    A failure moves the state on and decides the next wait, which
    :attr:`next_delay` reports. The delay for attempt ``n`` is
    ``min(min_delay * 2 ** (n - 1), max_delay)``, less a random jitter of up to
    half of it, and never less than the operating cadence.

    An unexpected failure moves the state to the extended regime, which raises
    both delay bounds. The bounds stay raised until the reset condition is met,
    so a normal failure that follows cannot lower them.
    """

    def __init__(
        self,
        initial_delay: float,
        normal_ceiling: float,
        extended_initial_delay: float,
        extended_ceiling: float,
        reset_policy: ResetPolicy,
        operating_cadence: float = 0,
    ):
        """
        :param initial_delay: the delay before the first retry, in seconds
        :param normal_ceiling: the longest normal-regime delay, in seconds
        :param extended_initial_delay: the delay before the first retry in the
            extended regime, in seconds
        :param extended_ceiling: the longest extended-regime delay, in seconds
        :param reset_policy: decides when the retry state resets
        :param operating_cadence: the rate the component normally operates at,
            in seconds; no wait is ever shorter than this. Zero disables the
            floor, which is what streaming wants.
        """
        self._initial_delay = initial_delay
        self._normal_ceiling = normal_ceiling
        self._extended_initial_delay = extended_initial_delay
        self._extended_ceiling = extended_ceiling
        self._reset_policy = reset_policy
        self._operating_cadence = operating_cadence

        self._n = 0
        self._extended = False
        self._min_delay = initial_delay
        self._max_delay = max(normal_ceiling, initial_delay)
        self._attempts = 0
        # Read before any outcome is recorded, this is the ordinary interval.
        self._next_delay = self._wait_between_operations()

    @property
    def next_delay(self) -> float:
        """The wait before the next attempt, in seconds, as the last recorded
        outcome decided it."""
        return self._next_delay

    @property
    def attempts(self) -> int:
        """How many failures since the last reset. For logging only."""
        return self._attempts

    @property
    def min_delay(self) -> float:
        """The delay the current regime starts from, in seconds."""
        return self._min_delay

    @property
    def max_delay(self) -> float:
        """The longest delay the current regime allows, in seconds."""
        return self._max_delay

    @property
    def operating_cadence(self) -> float:
        """The rate the component normally operates at, in seconds."""
        return self._operating_cadence

    @property
    def in_extended_regime(self) -> bool:
        """Whether an unexpected failure has moved this state to the extended
        delay bounds."""
        return self._extended

    def record_failure(self, kind: FailureKind) -> None:
        """
        Records a failed attempt, and decides the wait before the next one.

        The state moves on before the wait is computed, so :attr:`next_delay`
        always reflects the failure just recorded.

        :param kind: how the failure was classified
        """
        # Only a time-based policy needs this: nothing runs while a stream is healthy.
        self._reset_if_due()
        self._attempts += 1
        self._reset_policy.note_failure()

        if kind is FailureKind.UNEXPECTED and not self._extended:
            # Moving to the extended regime raises both bounds and starts the
            # delay sequence over. Only the move does this: a later unexpected
            # failure keeps counting up, so the delay is not pinned to the
            # extended initial delay.
            self._extended = True
            self._min_delay = self._extended_initial_delay
            self._max_delay = max(self._extended_ceiling, self._min_delay)
            self._n = 1
        else:
            self._n += 1

        self._next_delay = self._compute_wait()

    def record_success(self) -> None:
        """
        Records a successful operation, and resets the retry state if that is
        now enough.

        The wait before the next operation goes back to the ordinary interval,
        even when the retry state is still raised, because a backoff wait
        applies to a retry and not to every operation.
        """
        self._reset_policy.note_healthy()
        self._reset_if_due()
        self._next_delay = self._wait_between_operations()

    def _wait_between_operations(self) -> float:
        """The wait when nothing is being retried: the operating cadence, or
        the initial delay for a component that has no cadence."""
        return self._operating_cadence if self._operating_cadence > 0 else self._initial_delay

    def _reset_if_due(self) -> None:
        """Clears the retry state when the reset policy is satisfied, returning
        the delay bounds to the normal regime."""
        if not self._reset_policy.is_satisfied():
            return
        self._n = 0
        self._attempts = 0
        self._extended = False
        self._min_delay = self._initial_delay
        self._max_delay = max(self._normal_ceiling, self._initial_delay)

    def _compute_wait(self) -> float:
        exponent = min(max(self._n - 1, 0), _MAX_BACKOFF_EXPONENT)
        delay = min(self._min_delay * (2**exponent), self._max_delay)
        jitter = random.random() * delay / 2
        return max(delay - jitter, self._operating_cadence)


def _positive_finite(value: float, default: float, name: str) -> float:
    """Returns ``value`` if it is a positive, finite number of seconds, and the
    default otherwise. A non-finite value would make the jitter arithmetic
    produce a NaN delay, and a non-positive one would retry with no wait."""
    if value > 0 and math.isfinite(value):
        return value
    log.warning(
        "%s must be a positive, finite number of seconds; using the default of %ss"
        % (name, default)
    )
    return default


def for_streaming(initial_reconnect_delay: float) -> RetryState:
    """
    Builds the retry state for a streaming data source.

    Streaming has no operating cadence, so there is no floor on the wait. It
    is healthy from the first message of a fresh stream, and resets after a
    minute of that.

    ``Config`` does not check the configured delay, so the documented default
    stands in for anything that is not a positive, finite number.

    The extended regime never starts below the configured delay.
    """
    initial_reconnect_delay = _positive_finite(
        initial_reconnect_delay, DEFAULT_INITIAL_RECONNECT_DELAY, 'initial_reconnect_delay'
    )
    return RetryState(
        initial_delay=initial_reconnect_delay,
        normal_ceiling=STREAMING_MAX_DELAY,
        extended_initial_delay=max(EXTENDED_INITIAL_DELAY, initial_reconnect_delay),
        extended_ceiling=EXTENDED_MAX_DELAY,
        reset_policy=AfterHealthyFor(STREAMING_RESET_INTERVAL),
    )


def for_polling(poll_interval: float) -> RetryState:
    """
    Builds the retry state for a polling data source.

    The poll interval is polling's operating cadence, so no wait is ever
    shorter than it. In the normal regime the delay bounds are the poll
    interval itself, which means a normal failure simply polls again on
    schedule. Polling is healthy on any successful poll, and resets after two
    in a row.

    ``Config`` clamps the poll interval, but the documented default stands in
    for anything that reaches here and is not a positive, finite number.
    """
    poll_interval = _positive_finite(poll_interval, DEFAULT_POLL_INTERVAL, 'poll_interval')
    return RetryState(
        initial_delay=poll_interval,
        normal_ceiling=poll_interval,
        extended_initial_delay=max(EXTENDED_INITIAL_DELAY, poll_interval),
        extended_ceiling=EXTENDED_MAX_DELAY,
        reset_policy=AfterConsecutiveSuccesses(POLLING_RESET_SUCCESSES),
        operating_cadence=poll_interval,
    )
