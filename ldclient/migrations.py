from abc import ABCMeta, abstractmethod
from typing import Callable, Optional, Union, Set, Dict, Any
import time
from datetime import timedelta
from enum import Enum
from ldclient import Result, LDClient
from ldclient.evaluation import EvaluationDetail
from ldclient.context import Context
from ldclient.impl.model import FeatureFlag
from threading import Lock
from ldclient.impl.events.types import EventInputMigrationOp


MigratorFn = Callable[[Optional[Any]], Result]
"""
When a migration wishes to execute a read or write operation, it must delegate
that call to a consumer defined function. This function must accept an optional
payload value, and return a :class:`ldclient.Result`.
"""

MigratorCompareFn = Callable[[Any, Any], bool]
"""
If a migration read operation is executing which results in both origins being
read from, a customer defined comparison function may be used to determine if
the two results are equal.

This function should accept two parameters which represent the successful
result values of both the old and new origin reads. If the two values are
equal, this function should return true and false otherwise.
"""


class ExecutionOrder(Enum):
    """
    Depending on the migration stage, reads may operate against both old and
    new origins. In this situation, the execution order can be defined to
    specify how these individual reads are coordinated.
    """

    SERIAL = "serial"
    """
    SERIAL execution order ensures that the authoritative read completes before
    the non-authoritative read is executed.
    """

    RANDOM = "random"
    """
    Like SERIAL, RANDOM ensures that one read is completed before the
    subsequent read is executed. However, the order in which they are executed
    is randomly decided.
    """

    PARALLEL = "parallel"
    """
    PARALLEL executes both reads in separate threads. This helps reduce total
    run time at the cost of the thread overhead.
    """


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
        # TODO(sampling-ratio): Add sampling checking here
        with self.__mutex:
            self.__consistent = is_consistent()
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


class OperationResult:
    """
    The OperationResult wraps a :class:`ldclient.Result` pair an origin with a result.
    """

    def __init__(self, origin: Origin, result: Result):
        self.__origin = origin
        self.__result = result

    @property
    def origin(self) -> Origin:
        return self.__origin

    def __getattr__(self, attr):
        return getattr(self.wrappee, attr)


class Migrator:
    """
    A migrator is the interface through which migration support is executed. A
    migrator is configured through the :class:`MigratorBuilder`.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def read(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        """
        Uses the provided flag key and context to execute a migration-backed read operation.

        :param key: The migration flag key to use when determining the current stage
        :param context: The context to use when evaluating the flag
        :param default_stage: A default stage to fallback to if one cannot be determined
        :param payload: An optional payload to be passed through to the appropriate read method
        """

    @abstractmethod
    def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        """
        Uses the provided flag key and context to execute a migration-backed write operation.

        :param key: The migration flag key to use when determining the current stage
        :param context: The context to use when evaluating the flag
        :param default_stage: A default stage to fallback to if one cannot be determined
        :param payload: An optional payload to be passed through to the appropriate write method
        """


class MigrationConfig:
    """
    A migration config stores references to callable methods which execution
    customer defined read or write operations on old or new origins of
    information. For read operations, an optional comparison function also be
    defined.
    """

    def __init__(self, old: MigratorFn, new: MigratorFn, comparison: Optional[MigratorCompareFn] = None):
        self.__old = old
        self.__new = new
        self.__comparison = comparison

    @property
    def old(self) -> MigratorFn:
        """
        Callable which receives a nullable payload parameter and returns an
        :class:`ldclient.Result`.

        This function call should affect the old migration origin when called.

        @return [#call]
        """
        return self.__old

    @property
    def new(self) -> MigratorFn:
        """
        # Callable which receives a nullable payload parameter and returns an
        # :class:`ldclient.Result`.
        #
        # This function call should affect the new migration origin when
        # called.
        """
        return self.__new

    @property
    def comparison(self) -> Optional[MigratorCompareFn]:
        """
        Optional callable which receives two objects of any kind and returns a
        boolean representing equality.

        The result of this comparison can be sent upstream to LaunchDarkly to
        enhance migration observability.
        """
        return self.__comparison


class MigratorBuilder:
    """
    The migration builder is used to configure and construct an instance of a
    :class:`Migrator`. This migrator can be used to perform LaunchDarkly
    assisted technology migrations through the use of migration-based feature
    flags.
    """

    def __init__(self, client: LDClient):
        self.__client = client

        # Default settings as required by the spec
        self.__read_execution_order = ExecutionOrder.PARALLEL
        self.__measure_latency = True
        self.__measure_errors = True

        self.__read_config: Optional[MigrationConfig] = None
        self.__write_config: Optional[MigrationConfig] = None

    def read_execution_order(self, order: ExecutionOrder) -> 'MigratorBuilder':
        """
        The read execution order influences the parallelism and execution order
        for read operations involving multiple origins.
        """
        if order not in ExecutionOrder:
            return self

        self.__read_execution_order = order
        return self

    def track_latency(self, enabled: bool) -> 'MigratorBuilder':
        """
        Enable or disable latency tracking for migration operations. This
        latency information can be sent upstream to LaunchDarkly to enhance
        migration visibility.
        """
        self.__measure_latency = enabled
        return self

    def track_errors(self, enabled: bool) -> 'MigratorBuilder':
        """
        Enable or disable error tracking for migration operations. This error
        information can be sent upstream to LaunchDarkly to enhance migration
        visibility.
        """
        self.__measure_errors = enabled
        return self

    def read(self, old: MigratorFn, new: MigratorFn, comparison: Optional[MigratorCompareFn] = None) -> 'MigratorBuilder':
        """
        Read can be used to configure the migration-read behavior of the
        resulting :class:`Migrator` instance.

        Users are required to provide two different read methods -- one to read
        from the old migration origin, and one to read from the new origin.
        Additionally, customers can opt-in to consistency tracking by providing
        a comparison function.

        Depending on the migration stage, one or both of these read methods may
        be called.

        The read methods should accept a single nullable parameter. This
        parameter is a payload passed through the :func:`Migrator.read` method.
        This method should return a :class:`ldclient.Result` instance.

        The consistency method should accept 2 parameters of any type. These
        parameters are the results of executing the read operation against the
        old and new origins. If both operations were successful, the
        consistency method will be invoked. This method should return true if
        the two parameters are equal, or false otherwise.

        :param old: The function to execute when reading from the old origin
        :param new: The function to execute when reading from the new origin
        :param comparison: An optional function to use for comparing the results from two origins
        """
        self.__read_config = MigrationConfig(old, new, comparison)
        return self

    def write(self, old: MigratorFn, new: MigratorFn) -> 'MigratorBuilder':
        """
        Write can be used to configure the migration-write behavior of the
        resulting :class:`Migrator` instance.

        Users are required to provide two different write methods -- one to
        write to the old migration origin, and one to write to the new origin.

        Depending on the migration stage, one or both of these write methods
        may be called.

        The write methods should accept a single nullable parameter. This
        parameter is a payload passed through the :func:`Migrator.write`
        method. This method should return a :class:`ldclient.Result` instance.

        :param old: The function to execute when writing to the old origin
        :param new: The function to execute when writing to the new origin
        """
        self.__write_config = MigrationConfig(old, new)
        return self

    def build(self) -> Union[Migrator, str]:
        """
        Build constructs a :class:`Migrator` instance to support
        migration-based reads and writes. A string describing any failure
        conditions will be returned if the build fails.
        """
        if self.__read_config is None:
            return "read configuration not provided"

        if self.__write_config is None:
            return "write configuration not provided"

        from ldclient.impl.migrations import Migrator as MigratorImpl
        return MigratorImpl(
            self.__client,
            self.__read_execution_order,
            self.__read_config,
            self.__write_config,
            self.__measure_latency,
            self.__measure_errors,
        )
