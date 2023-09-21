from typing import Callable, Optional, Union, Set, Dict
import time
from datetime import timedelta
from enum import Enum
from ldclient.evaluation import EvaluationDetail
from ldclient.context import Context
from ldclient.impl.model import FeatureFlag
from threading import Lock
from ldclient.impl.events.types import EventInputMigrationOp


class Operation(Enum):
    """
    The operation enum is used to record the type of migration operation that
    occurred.
    """

    READ = "read"
    """
    READ represents a read-only operation on an origin of data.

    A read operation carries the implication that it can be executed in
    parallel against multiple origins.
    """

    WRITE = "write"
    """
    WRITE represents a write operation on an origin of data.

    A write operation implies that execution cannot be done in parallel against
    multiple origins.
    """


class Origin(Enum):
    """
    The origin enum is used to denote which source of data should be affected
    by a particular operation.
    """

    OLD = "old"
    """
    The OLD origin is the source of data we are migrating from. When the
    migration is complete, this source of data will be unused.
    """

    NEW = "new"
    """
    The NEW origin is the source of data we are migrating to. When the
    migration is complete, this source of data will be the source of truth.
    """


class Stage(Enum):
    """
    Stage denotes one of six possible stages a technology migration could be a
    part of, progressing through the following order.

    :class:`Stage.OFF` -> :class:`Stage.DUALWRITE` -> :class:`Stage.SHADOW` ->
    :class:`Stage.LIVE` -> :class:`Stage.RAMPDOWN` -> :class:`Stage.COMPLETE`
    """

    OFF = "off"
    """
    The migration hasn't started. :class:`Origin.OLD` is authoritative for
    reads and writes
    """

    DUALWRITE = "dualwrite"
    """
    Write to both :class:`Origin.OLD` and :class:`Origin.NEW`,
    :class:`Origin.OLD` is authoritative for reads
    """

    SHADOW = "shadow"
    """
    Both :class:`Origin.NEW` and :class:`Origin.OLD` versions run with
    a preference for :class:`Origin.OLD`
    """

    LIVE = "live"
    """
    Both :class:`Origin.NEW` and :class:`Origin.OLD` versions run with a
    preference for :class:`Origin.NEW`
    """

    RAMPDOWN = "rampdown"
    """
    Only read from :class:`Origin.NEW`, write to :class:`Origin.OLD` and
    :class:`Origin.NEW`
    """

    COMPLETE = "complete"
    """
    The migration is finished. :class:`Origin.NEW` is authoritative for reads
    and writes
    """


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

    def __init__(
        self, flag: FeatureFlag,
        context: Union[Context, dict],
        detail: EvaluationDetail,
        default_stage: Stage
    ):
        self.__flag = flag

        if not isinstance(context, Context):
            context = Context.from_dict(context)
        self.__context = context

        self.__detail = detail
        self.__default_stage = default_stage

        self.__mutex = Lock()

        self.__operation: Optional[Operation] = None
        self.__invoked: Set[Origin] = set()
        self.__consistent: Optional[bool] = None
        self.__consistent_ratio: Optional[int] = None  # TODO: Get this from the flag
        self.__errors: Set[Origin] = set()
        self.__latencies: Dict[Origin, timedelta] = {}

    def operation(self, op: Operation):
        """
        Sets the migration related operation associated with these tracking
        measurements.

        :param op: The read or write operation symbol.
        :return: :class:`OpTracker`
        """
        if not isinstance(op, Operation):
            return

        with self.__mutex:
            self.__operation = op
        return self

    def invoked(self, origin: Origin):
        """
        Allows recording which origins were called during a migration.

        :param origin: Designation for the old or new origin.
        :return: :class:`OpTracker`
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__invoked.add(origin)
        return self

    def consistent(self, is_consistent: Callable[[], bool]):
        """
        Allows recording the results of a consistency check.

        This method accepts a callable which should take no parameters and
        return a single boolean to represent the consistency check results for
        a read operation.

        A callable is provided in case sampling rules do not require
        consistency checking to run. In this case, we can avoid the overhead of
        a function by not using the callable.

        :param is_consistent: closure to return result of comparison check
        :return: :class:`OpTracker`
        """
        # TODO(sampling-ratio): Add sampling checking here
        with self.__mutex:
            self.__consistent = is_consistent()
        return self

    def error(self, origin: Origin):
        """
        Allows recording whether an error occurred during the operation.

        :param origin: Designation for the old or new origin.
        :return: :class:`OpTracker`
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__errors.add(origin)
        return self

    def latency(self, origin: Origin, duration: timedelta):
        """
        Allows tracking the recorded latency for an individual operation.

        :param origin: Designation for the old or new origin.
        :param duration: Duration measurement.
        :return: :class:`OpTracker`
        """
        if not isinstance(origin, Origin):
            return

        with self.__mutex:
            self.__latencies[origin] = duration
        return self

    def build(self) -> Union[EventInputMigrationOp, str]:
        """
        Creates an instance of :class:`ldclient.impl.EventInputMigrationOp()`.
        This event data can be provided to
        :func:`ldclient.client.LDClient.track_migration_op()` to rely this
        metric information upstream to LaunchDarkly services.

        :return: A :class:`ldclient.impl.EventInputMigrationOp()` or a string
            describing the type of failure.
        """
        with self.__mutex:
            if self.__operation is None:
                return "operation not provided"
            if len(self.__invoked) == 0:
                return "no origins were invoked"
            if not self.__context.valid:
                return "provided context was invalid"

            error = self.__check_invoked_consistency()
            if error:
                return error

            # TODO: Inject this time function or something
            timestamp = int(time.time() * 1_000)

            return EventInputMigrationOp(
                timestamp,
                self.__context,
                self.__flag,
                self.__operation,
                self.__default_stage,
                self.__detail,
                self.__invoked.copy(),
                self.__consistent,
                self.__consistent_ratio,
                self.__errors.copy(),
                self.__latencies.copy())

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
