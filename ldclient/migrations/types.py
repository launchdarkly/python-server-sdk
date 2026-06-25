from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar

from ldclient.impl.util import Result

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

    @staticmethod
    def from_str(order: str) -> Optional['ExecutionOrder']:
        """
        This method will create a Stage enum corresponding to the given string.
        If the string doesn't map to a stage, None will returned.
        """
        try:
            return next(e for e in ExecutionOrder if e.value == order)
        except StopIteration:
            return None


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

    @staticmethod
    def from_str(stage: str) -> Optional['Stage']:
        """
        This method will create a Stage enum corresponding to the given string.
        If the string doesn't map to a stage, OFF will be used.
        """
        try:
            return next(s for s in Stage if s.value == stage)
        except StopIteration:
            return None


class OperationResult(Result):
    """
    The OperationResult wraps a :class:`ldclient.Result` pair an origin with a result.
    """

    def __init__(self, origin: Origin, result: Result):
        super().__init__(result.value, result.error, result.exception)
        self.__origin = origin

    @property
    def origin(self) -> Origin:
        return self.__origin


class WriteResult:
    """
    A write result contains the operation results against both the
    authoritative and non-authoritative origins.

    Authoritative writes are always executed first. In the event of a failure,
    the non-authoritative write will not be executed, resulting in a None value
    in the final WriteResult.
    """

    def __init__(self, authoritative: OperationResult, nonauthoritative: Optional[OperationResult] = None):
        self.__authoritative = authoritative
        self.__nonauthoritative = nonauthoritative

    @property
    def authoritative(self) -> OperationResult:
        return self.__authoritative

    @property
    def nonauthoritative(self) -> Optional[OperationResult]:
        return self.__nonauthoritative


_MigratorFnT = TypeVar('_MigratorFnT')


class _MigrationConfigBase(Generic[_MigratorFnT]):
    """
    Shared implementation backing :class:`MigrationConfig` and its async
    counterpart. It stores references to the customer defined read or write
    functions for the old and new origins, along with an optional synchronous
    comparison function used for read consistency tracking.
    """

    def __init__(self, old: _MigratorFnT, new: _MigratorFnT, comparison: Optional[MigratorCompareFn] = None):
        self._old = old
        self._new = new
        self._comparison = comparison

    @property
    def old(self) -> _MigratorFnT:
        """
        Callable which receives a nullable payload parameter and returns an
        :class:`ldclient.Result`.

        This function call should affect the old migration origin when called.
        """
        return self._old

    @property
    def new(self) -> _MigratorFnT:
        """
        Callable which receives a nullable payload parameter and returns an
        :class:`ldclient.Result`.

        This function call should affect the new migration origin when called.
        """
        return self._new

    @property
    def comparison(self) -> Optional[MigratorCompareFn]:
        """
        Optional callable which receives two objects of any kind and returns a
        boolean representing equality.

        The result of this comparison can be sent upstream to LaunchDarkly to
        enhance migration observability.
        """
        return self._comparison


class MigrationConfig(_MigrationConfigBase[MigratorFn]):
    """
    A migration config stores references to callable methods which execute
    customer defined read or write operations on old or new origins of
    information. For read operations, an optional comparison function also be
    defined.
    """


_MigratorBuilderT = TypeVar('_MigratorBuilderT', bound='_MigratorBuilderBase')


class _MigratorBuilderBase:
    """
    Shared setter implementation for :class:`MigratorBuilder` and its async
    counterpart. It holds the fluent configuration methods common to both
    builders. The ``read``, ``write``, and ``build`` methods differ between the
    sync and async builders and are defined on each subclass.
    """

    _read_execution_order: ExecutionOrder
    _measure_latency: bool
    _measure_errors: bool

    def read_execution_order(self: _MigratorBuilderT, order: ExecutionOrder) -> _MigratorBuilderT:
        """
        The read execution order influences the parallelism and execution order
        for read operations involving multiple origins.
        """
        if order not in ExecutionOrder:
            return self

        self._read_execution_order = order
        return self

    def track_latency(self: _MigratorBuilderT, enabled: bool) -> _MigratorBuilderT:
        """
        Enable or disable latency tracking for migration operations. This
        latency information can be sent upstream to LaunchDarkly to enhance
        migration visibility.
        """
        self._measure_latency = enabled
        return self

    def track_errors(self: _MigratorBuilderT, enabled: bool) -> _MigratorBuilderT:
        """
        Enable or disable error tracking for migration operations. This error
        information can be sent upstream to LaunchDarkly to enhance migration
        visibility.
        """
        self._measure_errors = enabled
        return self
