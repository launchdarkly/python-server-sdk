import time
from datetime import timedelta
from random import Random
from threading import Lock
from typing import Callable, Dict, Optional, Set, Union

from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.types import EventInput
from ldclient.impl.model import FeatureFlag
from ldclient.impl.sampler import Sampler
from ldclient.impl.util import log
from ldclient.migrations.types import Operation, Origin, Stage


class MigrationOpEvent(EventInput):
    """
    A migration op event represents the results of a migration-assisted read or
    write operation.

    The event includes optional measurements reporting on consistency checks,
    error reporting, and operation latency values.

    This event should not be constructed directly; rather, it should be built
    through :class:`ldclient.migrations.OpTracker()`.
    """

    __slots__ = ['key', 'flag', 'operation', 'default_stage', 'detail', 'invoked', 'consistent', 'consistent_ratio', 'errors', 'latencies']

    def __init__(
        self,
        timestamp: int,
        context: Context,
        key: str,
        flag: Optional[FeatureFlag],
        operation: Operation,
        default_stage: Stage,
        detail: EvaluationDetail,
        invoked: Set[Origin],
        consistent: Optional[bool],
        consistent_ratio: Optional[int],
        errors: Set[Origin],
        latencies: Dict[Origin, timedelta],
    ):
        sampling_ratio = None if flag is None else flag.sampling_ratio
        super().__init__(timestamp, context, sampling_ratio)

        self.key = key
        self.flag = flag
        self.operation = operation
        self.default_stage = default_stage
        self.detail = detail
        self.invoked = invoked
        self.consistent = consistent
        self.consistent_ratio = consistent_ratio
        self.errors = errors
        self.latencies = latencies

    def to_debugging_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "context": self.context.to_dict(),
            "flag": None if self.flag is None else {"key": self.flag.key},
            "operation": self.operation.value,
            "default_stage": self.default_stage.value,
            "detail": self.detail,
            "invoked": self.invoked,
            "consistent": self.consistent,
            "consistent_ratio": self.consistent_ratio,
            "errors": self.errors,
            "latencies": self.latencies,
        }


class OpTracker:
    """
    An OpTracker is responsible for managing the collection of measurements
    that which a user might wish to record throughout a migration-assisted
    operation.

    Example measurements include latency, errors, and consistency.

    The OpTracker is not expected to be instantiated directly. Consumers should
    instead call :func:`ldclient.client.LDClient.migration_variation()` and use
    the returned tracker instance.
    """

    def __init__(self, key: str, flag: Optional[FeatureFlag], context: Context, detail: EvaluationDetail, default_stage: Stage):
        self.__key = key
        self.__flag = flag
        self.__context = context
        self.__detail = detail
        self.__default_stage = default_stage

        self.__mutex = Lock()

        self.__operation: Optional[Operation] = None
        self.__invoked: Set[Origin] = set()
        self.__consistent: Optional[bool] = None

        self.__consistent_ratio: int = 1
        if flag is not None and flag.migrations is not None and flag.migrations.check_ratio is not None:
            self.__consistent_ratio = flag.migrations.check_ratio

        self.__errors: Set[Origin] = set()
        self.__latencies: Dict[Origin, timedelta] = {}

        self.__sampler = Sampler(Random())

    def operation(self, op: Operation) -> 'OpTracker':
        """
        Sets the migration related operation associated with these tracking
        measurements.

        :param op: The read or write operation symbol.
        """
        if not isinstance(op, Operation):
            return self

        with self.__mutex:
            self.__operation = op
        return self

    def invoked(self, origin: Origin) -> 'OpTracker':
        """
        Allows recording which origins were called during a migration.

        :param origin: Designation for the old or new origin.
        """
        if not isinstance(origin, Origin):
            return self

        with self.__mutex:
            self.__invoked.add(origin)
        return self

    def consistent(self, is_consistent: Callable[[], bool]) -> 'OpTracker':
        """
        Allows recording the results of a consistency check.

        This method accepts a callable which should take no parameters and
        return a single boolean to represent the consistency check results for
        a read operation.

        A callable is provided in case sampling rules do not require
        consistency checking to run. In this case, we can avoid the overhead of
        a function by not using the callable.

        :param is_consistent: closure to return result of comparison check
        """
        with self.__mutex:
            try:
                if self.__sampler.sample(self.__consistent_ratio):
                    self.__consistent = is_consistent()
            except Exception as e:
                log.error("exception raised during consistency check %s; failed to record measurement", repr(e))

        return self

    def error(self, origin: Origin) -> 'OpTracker':
        """
        Allows recording whether an error occurred during the operation.

        :param origin: Designation for the old or new origin.
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__errors.add(origin)
        return self

    def latency(self, origin: Origin, duration: timedelta) -> 'OpTracker':
        """
        Allows tracking the recorded latency for an individual operation.

        :param origin: Designation for the old or new origin.
        :param duration: Duration measurement.
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__latencies[origin] = duration
        return self

    def build(self) -> Union[MigrationOpEvent, str]:
        """
        Creates an instance of :class:`MigrationOpEvent()`.
        This event data can be provided to
        :func:`ldclient.client.LDClient.track_migration_op()` to relay this
        metric information upstream to LaunchDarkly services.

        :return: A :class:`MigrationOpEvent()` or a string
            describing the type of failure.
        """
        with self.__mutex:
            if self.__operation is None:
                return "operation not provided"
            if len(self.__key) == 0:
                return "migration operation cannot contain an empty key"
            if len(self.__invoked) == 0:
                return "no origins were invoked"
            if not self.__context.valid:
                return "provided context was invalid"

            error = self.__check_invoked_consistency()
            if error:
                return error

            # TODO: Inject this time function or something
            timestamp = int(time.time() * 1_000)

            return MigrationOpEvent(
                timestamp,
                self.__context,
                self.__key,
                self.__flag,
                self.__operation,
                self.__default_stage,
                self.__detail,
                self.__invoked.copy(),
                self.__consistent,
                None if self.__consistent is None else self.__consistent_ratio,
                self.__errors.copy(),
                self.__latencies.copy(),
            )

    def __check_invoked_consistency(self) -> Optional[str]:
        for origin in Origin:
            if origin in self.__invoked:
                continue

            if origin in self.__latencies:
                return f"provided latency for origin '{origin.value}' without recording invocation"
            if origin in self.__errors:
                return f"provided error for origin '{origin.value}' without recording invocation"

        # A consistency measurement only makes sense if TWO origins were
        # executed. Otherwise, there is nothing to compare against.
        if self.__consistent is not None and len(self.__invoked) != 2:
            return "provided consistency without recording both invocations"

        return None
