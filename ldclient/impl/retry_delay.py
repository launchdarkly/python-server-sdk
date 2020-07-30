from random import Random

# This implementation is based on the equivalent code in the Go eventsource library.

class RetryDelayStrategy:
    """Encapsulation of configurable backoff/jitter behavior, used for stream connections.

    - The system can either be in a "good" state or a "bad" state. The initial state is "bad"; the
    caller is responsible for indicating when it transitions to "good". When we ask for a new retry
    delay, that implies the state is now transitioning to "bad".

    - There is a configurable base delay, which can be changed at any time (if the SSE server sends
    us a "retry:" directive).

    - There are optional strategies for applying backoff and jitter to the delay.

    This object is meant to be used from a single thread once it's been created; its methods are
    not safe for concurrent use.
    """
    def __init__(self, base_delay, reset_interval, backoff_strategy, jitter_strategy):
        self.__base_delay = base_delay
        self.__reset_interval = reset_interval
        self.__backoff = backoff_strategy
        self.__jitter = jitter_strategy
        self.__retry_count = 0
        self.__good_since = None

    def next_retry_delay(self, current_time):
        """Computes the next retry interval. This also sets the current state to "bad".

        Note that current_time is passed as a parameter instead of computed by this function to
        guarantee predictable behavior in tests.

        :param float current_time: the current time, in seconds
        """
        if self.__good_since and self.__reset_interval and (current_time - self.__good_since >= self.__reset_interval):
            self.__retry_count = 0
        self.__good_since = None
        delay = self.__base_delay
        if self.__backoff:
            delay = self.__backoff.apply_backoff(delay, self.__retry_count)
        self.__retry_count += 1
        if self.__jitter:
            delay = self.__jitter.apply_jitter(delay)
        return delay

    def set_good_since(self, good_since):
        """Marks the current state as "good" and records the time.

        :param float good_since: the time that the state became "good", in seconds
        """
        self.__good_since = good_since

    def set_base_delay(self, base_delay):
        """Changes the initial retry delay and resets the backoff (if any) so the next retry will use
        that value.

        This is used to implement the optional SSE behavior where the server sends a "retry:" command to
        set the base retry to a specific value. Note that we will still apply a jitter, if jitter is enabled,
        and subsequent retries will still increase exponentially.
        """
        self.__base_delay = base_delay
        self.__retry_count = 0

class DefaultBackoffStrategy:
    """The default implementation of exponential backoff, which doubles the delay each time up to
    the specified maximum.

    If a reset_interval was specified for the RetryDelayStrategy, and the system has been in a "good"
    state for at least that long, the delay is reset back to the base. This avoids perpetually increasing
    delays in a situation where failures are rare).
    """
    def __init__(self, max_delay):
        self.__max_delay = max_delay

    def apply_backoff(self, delay, retry_count):
        d = delay * (2 ** retry_count)
        return d if d <= self.__max_delay else self.__max_delay

class DefaultJitterStrategy:
    """The default implementation of jitter, which subtracts a pseudo-random amount from each delay.
    """
    def __init__(self, ratio, rand_seed = None):
        """Creates an instance.

        :param float ratio: a number in the range [0.0, 1.0] representing 0%-100% jitter
        :param int rand_seed: if not None, will use this random seed (for test determinacy)
        """
        self.__ratio = ratio
        self.__random = Random(rand_seed)

    def apply_jitter(self, delay):
        return delay - (self.__random.random() * self.__ratio * delay)
