from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from random import Random
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    Union
)

from ldclient.impl.sampler import Sampler
from ldclient.impl.util import Result
from ldclient.migrations.tracker import OpTracker
from ldclient.migrations.types import (
    ExecutionOrder,
    MigrationConfig,
    MigratorCompareFn,
    Operation,
    OperationResult,
    Origin,
    Stage,
    WriteResult,
    _MigrationConfigBase,
    _MigratorBuilderBase
)

if TYPE_CHECKING:
    from ldclient import Context
    from ldclient.async_client import AsyncLDClient

__all__ = [
    'AsyncMigrator',
    'AsyncMigratorBuilder',
    'AsyncMigratorImpl',
    'AsyncMigrationConfig',
    'AsyncExecutor',
    'AsyncMigratorFn',
]

AsyncMigratorFn = Callable[[Optional[Any]], Awaitable[Any]]
"""
The async counterpart to :data:`ldclient.migrations.MigratorFn`. When an async
migration wishes to execute a read or write operation, it must delegate that
call to a consumer defined coroutine function. This function must accept an
optional payload value, and return a :class:`ldclient.Result`.
"""


class AsyncMigrator(ABC):
    """
    An async migrator is the interface through which migration support is
    executed for the async SDK. An async migrator is configured through the
    :class:`AsyncMigratorBuilder`.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.
    """

    @abstractmethod
    async def read(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        """
        Uses the provided flag key and context to execute a migration-backed read operation.

        :param key: The migration flag key to use when determining the current stage
        :param context: The context to use when evaluating the flag
        :param default_stage: A default stage to fallback to if one cannot be determined
        :param payload: An optional payload to be passed through to the appropriate read method
        """

    @abstractmethod
    async def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> WriteResult:
        """
        Uses the provided flag key and context to execute a migration-backed write operation.

        :param key: The migration flag key to use when determining the current stage
        :param context: The context to use when evaluating the flag
        :param default_stage: A default stage to fallback to if one cannot be determined
        :param payload: An optional payload to be passed through to the appropriate write method
        """


class AsyncMigratorImpl(AsyncMigrator):
    """
    An implementation of the :class:`ldclient.migrations.AsyncMigrator`
    interface, capable of supporting feature-flag backed technology migrations
    for the async SDK.
    """

    def __init__(
        self,
        sampler: Sampler,
        client: AsyncLDClient,
        read_execution_order: ExecutionOrder,
        read_config: AsyncMigrationConfig,
        write_config: AsyncMigrationConfig,
        measure_latency: bool,
        measure_errors: bool,
    ):
        self._sampler = sampler
        self._client = client
        self._read_execution_order = read_execution_order
        self._read_config = read_config
        self._write_config = write_config
        self._measure_latency = measure_latency
        self._measure_errors = measure_errors

    async def read(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        stage, tracker = await self._client.migration_variation(key, context, default_stage)
        tracker.operation(Operation.READ)

        old = AsyncExecutor(Origin.OLD, self._read_config.old, tracker, self._measure_latency, self._measure_errors, payload)
        new = AsyncExecutor(Origin.NEW, self._read_config.new, tracker, self._measure_latency, self._measure_errors, payload)

        if stage == Stage.OFF:
            result = await old.run()
        elif stage == Stage.DUALWRITE:
            result = await old.run()
        elif stage == Stage.SHADOW:
            result = await self.__read_both(old, new, tracker)
        elif stage == Stage.LIVE:
            result = await self.__read_both(new, old, tracker)
        elif stage == Stage.RAMPDOWN:
            result = await new.run()
        else:
            result = await new.run()

        # track_migration_op is synchronous on the async client; do not await it.
        self._client.track_migration_op(tracker)

        return result

    async def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> WriteResult:
        stage, tracker = await self._client.migration_variation(key, context, default_stage)
        tracker.operation(Operation.WRITE)

        old = AsyncExecutor(Origin.OLD, self._write_config.old, tracker, self._measure_latency, self._measure_errors, payload)
        new = AsyncExecutor(Origin.NEW, self._write_config.new, tracker, self._measure_latency, self._measure_errors, payload)

        if stage == Stage.OFF:
            result = await old.run()
            write_result = WriteResult(result)
        elif stage == Stage.DUALWRITE:
            authoritative_result, nonauthoritative_result = await self.__write_both(old, new, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.SHADOW:
            authoritative_result, nonauthoritative_result = await self.__write_both(old, new, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.LIVE:
            authoritative_result, nonauthoritative_result = await self.__write_both(new, old, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.RAMPDOWN:
            authoritative_result, nonauthoritative_result = await self.__write_both(new, old, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        else:
            result = await new.run()
            write_result = WriteResult(result)

        # track_migration_op is synchronous on the async client; do not await it.
        self._client.track_migration_op(tracker)

        return write_result

    async def __read_both(self, authoritative: AsyncExecutor, nonauthoritative: AsyncExecutor, tracker: OpTracker) -> OperationResult:
        if self._read_execution_order == ExecutionOrder.PARALLEL:
            authoritative_result, nonauthoritative_result = await asyncio.gather(
                authoritative.run(),
                nonauthoritative.run(),
            )
        elif self._read_execution_order == ExecutionOrder.RANDOM and self._sampler.sample(2):
            nonauthoritative_result = await nonauthoritative.run()
            authoritative_result = await authoritative.run()
        else:
            authoritative_result = await authoritative.run()
            nonauthoritative_result = await nonauthoritative.run()

        if self._read_config.comparison is None:
            return authoritative_result

        compare = self._read_config.comparison
        if authoritative_result.is_success() and nonauthoritative_result.is_success():
            tracker.consistent(lambda: compare(authoritative_result.value, nonauthoritative_result.value))

        return authoritative_result

    async def __write_both(self, authoritative: AsyncExecutor, nonauthoritative: AsyncExecutor, tracker: OpTracker) -> Tuple[OperationResult, Optional[OperationResult]]:
        authoritative_result = await authoritative.run()
        tracker.invoked(authoritative.origin)

        if not authoritative_result.is_success():
            return authoritative_result, None

        nonauthoritative_result = await nonauthoritative.run()
        tracker.invoked(nonauthoritative.origin)

        return authoritative_result, nonauthoritative_result


class AsyncMigrationConfig(_MigrationConfigBase[AsyncMigratorFn]):
    """
    The async counterpart to :class:`ldclient.migrations.MigrationConfig`. It
    stores references to coroutine functions which execute customer defined
    read or write operations on old or new origins of information. For read
    operations, an optional (synchronous) comparison function can also be
    defined.
    """


class AsyncMigratorBuilder(_MigratorBuilderBase):
    """
    The async migration builder is used to configure and construct an instance
    of an :class:`AsyncMigrator`. This migrator can be used to perform
    LaunchDarkly assisted technology migrations through the use of
    migration-based feature flags.

    .. caution::
        This feature is experimental and should NOT be considered ready for production
        use. It may change or be removed without notice and is not subject to backwards
        compatibility guarantees. Pin to a specific minor version and review the changelog
        before upgrading.
    """

    def __init__(self, client: AsyncLDClient):
        # Single _ to prevent mangling; useful for testing
        self._client = client

        # Default settings as required by the spec
        self._read_execution_order = ExecutionOrder.PARALLEL
        self._measure_latency = True
        self._measure_errors = True

        self.__read_config: Optional[AsyncMigrationConfig] = None
        self.__write_config: Optional[AsyncMigrationConfig] = None

    def read(self, old: AsyncMigratorFn, new: AsyncMigratorFn, comparison: Optional[MigratorCompareFn] = None) -> 'AsyncMigratorBuilder':
        """
        Read can be used to configure the migration-read behavior of the
        resulting :class:`AsyncMigrator` instance.

        Users are required to provide two different read coroutine functions --
        one to read from the old migration origin, and one to read from the new
        origin. Additionally, customers can opt-in to consistency tracking by
        providing a comparison function.

        Depending on the migration stage, one or both of these read methods may
        be called.

        The read methods should accept a single nullable parameter. This
        parameter is a payload passed through the :func:`AsyncMigrator.read`
        method. This method should return a :class:`ldclient.Result` instance.

        The consistency method should accept 2 parameters of any type. These
        parameters are the results of executing the read operation against the
        old and new origins. If both operations were successful, the
        consistency method will be invoked. This method should return true if
        the two parameters are equal, or false otherwise. The comparison
        function is synchronous.

        :param old: The coroutine function to execute when reading from the old origin
        :param new: The coroutine function to execute when reading from the new origin
        :param comparison: An optional function to use for comparing the results from two origins
        """
        self.__read_config = AsyncMigrationConfig(old, new, comparison)
        return self

    def write(self, old: AsyncMigratorFn, new: AsyncMigratorFn) -> 'AsyncMigratorBuilder':
        """
        Write can be used to configure the migration-write behavior of the
        resulting :class:`AsyncMigrator` instance.

        Users are required to provide two different write coroutine functions --
        one to write to the old migration origin, and one to write to the new
        origin.

        Depending on the migration stage, one or both of these write methods
        may be called.

        The write methods should accept a single nullable parameter. This
        parameter is a payload passed through the :func:`AsyncMigrator.write`
        method. This method should return a :class:`ldclient.Result` instance.

        :param old: The coroutine function to execute when writing to the old origin
        :param new: The coroutine function to execute when writing to the new origin
        """
        self.__write_config = AsyncMigrationConfig(old, new)
        return self

    def build(self) -> Union[AsyncMigrator, str]:
        """
        Build constructs an :class:`AsyncMigrator` instance to support
        migration-based reads and writes. A string describing any failure
        conditions will be returned if the build fails.
        """
        if self.__read_config is None:
            return "read configuration not provided"

        if self.__write_config is None:
            return "write configuration not provided"

        return AsyncMigratorImpl(
            Sampler(Random()),
            self._client,
            self._read_execution_order,
            self.__read_config,
            self.__write_config,
            self._measure_latency,
            self._measure_errors,
        )


class AsyncExecutor:
    """
    Utility class for executing async migration operations while also tracking
    our built-in migration measurements.
    """

    def __init__(self, origin: Origin, fn: AsyncMigratorFn, tracker: OpTracker, measure_latency: bool, measure_errors: bool, payload: Any):
        self.__origin = origin
        self.__fn = fn
        self.__tracker = tracker
        self.__measure_latency = measure_latency
        self.__measure_errors = measure_errors
        self.__payload = payload

    @property
    def origin(self) -> Origin:
        return self.__origin

    async def run(self) -> OperationResult:
        """
        Execute the configured operation and track any available measurements.
        """
        start = datetime.now()

        try:
            result = await self.__fn(self.__payload)
        except Exception as e:
            result = Result.fail(f"'{self.__origin.value} operation raised an exception", e)

        # Record required tracker measurements
        if self.__measure_latency:
            self.__tracker.latency(self.__origin, datetime.now() - start)

        if self.__measure_errors and not result.is_success():
            self.__tracker.error(self.__origin)

        self.__tracker.invoked(self.__origin)

        return OperationResult(self.__origin, result)
