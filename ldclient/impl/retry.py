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

from ldclient.config import (
    DEFAULT_INITIAL_RECONNECT_DELAY,
    DEFAULT_POLL_INTERVAL
)
from ldclient.impl.util import log

# The delay ceiling of the normal regime for streaming, in seconds.
NORMAL_STREAMING_CEILING_DELAY = 30

# The delay bounds of the extended regime, in seconds. A component enters the
# extended regime after an unexpected failure.
EXTENDED_INITIAL_DELAY = 5 * 60
EXTENDED_CEILING_DELAY = 60 * 60

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


def _usable_delay(value: float, default: float, name: str, ceiling: float = math.inf) -> float:
    """
    Returns the delay to use, clamped to the ceiling. A value that is
    not a positive, finite number of seconds is replaced by the default.

    :param value: the configured number of seconds
    :param default: the value to use when ``value`` is not usable
    :param name: the option name, for the warning message
    :param ceiling: the longest delay allowed
    """

    if value > 0 and math.isfinite(value):
        return min(value, ceiling)
    log.warning("%s must be a positive, finite number of seconds; using the default of %ss" % (name, default))
    return default


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


class RetryState:
    """
    Tracks how long a data source should wait before its next attempt.

    A failure moves the state on and decides the next wait, which
    :attr:`next_delay` reports. The delay is
    ``min(min_delay * 2 ** (attempts - 1), max_delay)``, less a random jitter
    of up to half of it, and never less than the operating cadence.

    An unexpected failure moves the state to the extended regime, which raises
    both delay bounds. The bounds stay raised until the reset condition is met,
    so a normal failure that follows cannot lower them.
    """

    def __init__(
        self,
        normal_initial_delay: float,
        normal_ceiling_delay: float,
        extended_initial_delay: float,
        extended_ceiling_delay: float,
        reset_policy: ResetPolicy,
        operating_cadence: float = 0,
    ):
        """
        :param normal_initial_delay: the delay before the first retry in the
            normal regime, in seconds
        :param normal_ceiling_delay: the longest normal-regime delay, in
            seconds
        :param extended_initial_delay: the delay before the first retry in the
            extended regime, in seconds
        :param extended_ceiling_delay: the longest extended-regime delay, in
            seconds
        :param reset_policy: decides when the retry state resets
        :param operating_cadence: the wait between healthy operations, in
            seconds; no wait is ever shorter than this. Zero for a component
            that operates continuously.
        """
        self._normal_initial_delay = normal_initial_delay
        self._normal_ceiling_delay = normal_ceiling_delay
        self._extended_initial_delay = extended_initial_delay
        self._extended_ceiling_delay = extended_ceiling_delay
        self._reset_policy = reset_policy
        self._operating_cadence = operating_cadence

        self._attempts = 0
        self._extended = False
        self._min_delay = self._normal_initial_delay
        self._max_delay = max(self._normal_ceiling_delay, self._normal_initial_delay)
        # Read before any outcome is recorded, this is the ordinary interval.
        self._next_delay = self._operating_cadence

    @property
    def next_delay(self) -> float:
        """The wait before the next operation, in seconds, as the last recorded
        outcome decided it."""
        return self._next_delay

    def record_failure(self, kind: FailureKind) -> None:
        """
        Records a failed attempt, and decides the wait before the next one.

        The state moves on before the wait is computed, so :attr:`next_delay`
        always reflects the failure just recorded.

        :param kind: how the failure was classified
        """
        # Only a time-based policy needs this: nothing runs while a stream is healthy.
        self._reset_if_due()
        self._reset_policy.note_failure()

        if kind is FailureKind.UNEXPECTED and not self._extended:
            # Moving to the extended regime raises both bounds and starts the
            # delay sequence over. Only the move does this: a later unexpected
            # failure keeps counting up, so the delay is not pinned to the
            # extended initial delay.
            self._extended = True
            self._min_delay = self._extended_initial_delay
            self._max_delay = max(self._extended_ceiling_delay, self._min_delay)
            self._attempts = 1
        else:
            self._attempts += 1

        exponent = min(max(self._attempts - 1, 0), _MAX_BACKOFF_EXPONENT)
        delay = min(self._min_delay * (2**exponent), self._max_delay)
        jitter = random.random() * delay / 2

        self._next_delay = max(delay - jitter, self._operating_cadence)

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
        self._next_delay = self._operating_cadence

    def _reset_if_due(self) -> None:
        """Clears the retry state when the reset policy is satisfied, returning
        the delay bounds to the normal regime."""
        if not self._reset_policy.is_satisfied():
            return
        self._attempts = 0
        self._extended = False
        self._min_delay = self._normal_initial_delay
        self._max_delay = max(self._normal_ceiling_delay, self._normal_initial_delay)


def for_streaming(initial_reconnect_delay: float) -> RetryState:
    """
    Builds the retry state for a streaming data source.

    Streaming's cadence is zero, so a healthy stream never waits. An invalid
    delay value is replaced by the documented default; one longer than a
    ceiling raises that bound rather than being cut down to it.
    """
    initial_reconnect_delay = _usable_delay(
        initial_reconnect_delay, DEFAULT_INITIAL_RECONNECT_DELAY, 'initial_reconnect_delay'
    )
    return RetryState(
        normal_initial_delay=initial_reconnect_delay,
        normal_ceiling_delay=NORMAL_STREAMING_CEILING_DELAY,
        extended_initial_delay=max(EXTENDED_INITIAL_DELAY, initial_reconnect_delay),
        extended_ceiling_delay=EXTENDED_CEILING_DELAY,
        reset_policy=AfterHealthyFor(STREAMING_RESET_INTERVAL),
        operating_cadence=0
    )


def for_polling(poll_interval: float) -> RetryState:
    """
    Builds the retry state for a polling data source.

    The poll interval is polling's cadence and its normal ceiling, so a normal
    failure waits the interval rather than backing off past it. An invalid
    interval is replaced by the documented default. No wait is ever shorter
    than the interval, so the cadence wins over the extended ceiling.
    """
    poll_interval = _usable_delay(poll_interval, DEFAULT_POLL_INTERVAL, 'poll_interval')
    return RetryState(
        normal_initial_delay=poll_interval,
        normal_ceiling_delay=poll_interval,
        extended_initial_delay=max(EXTENDED_INITIAL_DELAY, poll_interval),
        extended_ceiling_delay=EXTENDED_CEILING_DELAY,
        reset_policy=AfterConsecutiveSuccesses(POLLING_RESET_SUCCESSES),
        operating_cadence=poll_interval,
    )
