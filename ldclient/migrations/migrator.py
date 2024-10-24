from __future__ import annotations

import concurrent.futures
from abc import ABCMeta, abstractmethod
from datetime import datetime
from random import Random
from typing import TYPE_CHECKING, Any, Optional, Tuple, Union

from ldclient.impl.sampler import Sampler
from ldclient.impl.util import Result
from ldclient.migrations.tracker import OpTracker
from ldclient.migrations.types import (ExecutionOrder, MigrationConfig,
                                       MigratorCompareFn, MigratorFn,
                                       Operation, OperationResult, Origin,
                                       Stage, WriteResult)

if TYPE_CHECKING:
    from ldclient import Context, LDClient


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
    def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> WriteResult:
        """
        Uses the provided flag key and context to execute a migration-backed write operation.

        :param key: The migration flag key to use when determining the current stage
        :param context: The context to use when evaluating the flag
        :param default_stage: A default stage to fallback to if one cannot be determined
        :param payload: An optional payload to be passed through to the appropriate write method
        """


class MigratorImpl(Migrator):
    """
    An implementation of :class:`ldclient.migrations.Migrator` interface,
    capable of supporting feature-flag backed technology migrations.
    """

    def __init__(
        self, sampler: Sampler, client: LDClient, read_execution_order: ExecutionOrder, read_config: MigrationConfig, write_config: MigrationConfig, measure_latency: bool, measure_errors: bool
    ):
        self.__sampler = sampler
        self.__client = client
        self.__read_execution_order = read_execution_order
        self.__read_config = read_config
        self.__write_config = write_config
        self.__measure_latency = measure_latency
        self.__measure_errors = measure_errors

    def read(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        stage, tracker = self.__client.migration_variation(key, context, default_stage)
        tracker.operation(Operation.READ)

        old = Executor(Origin.OLD, self.__read_config.old, tracker, self.__measure_latency, self.__measure_errors, payload)
        new = Executor(Origin.NEW, self.__read_config.new, tracker, self.__measure_latency, self.__measure_errors, payload)

        if stage == Stage.OFF:
            result = old.run()
        elif stage == Stage.DUALWRITE:
            result = old.run()
        elif stage == Stage.SHADOW:
            result = self.__read_both(old, new, tracker)
        elif stage == Stage.LIVE:
            result = self.__read_both(new, old, tracker)
        elif stage == Stage.RAMPDOWN:
            result = new.run()
        else:
            result = new.run()

        self.__client.track_migration_op(tracker)

        return result

    def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> WriteResult:
        stage, tracker = self.__client.migration_variation(key, context, default_stage)
        tracker.operation(Operation.WRITE)

        old = Executor(Origin.OLD, self.__write_config.old, tracker, self.__measure_latency, self.__measure_errors, payload)
        new = Executor(Origin.NEW, self.__write_config.new, tracker, self.__measure_latency, self.__measure_errors, payload)

        if stage == Stage.OFF:
            result = old.run()
            write_result = WriteResult(result)
        elif stage == Stage.DUALWRITE:
            authoritative_result, nonauthoritative_result = self.__write_both(old, new, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.SHADOW:
            authoritative_result, nonauthoritative_result = self.__write_both(old, new, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.LIVE:
            authoritative_result, nonauthoritative_result = self.__write_both(new, old, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        elif stage == Stage.RAMPDOWN:
            authoritative_result, nonauthoritative_result = self.__write_both(new, old, tracker)
            write_result = WriteResult(authoritative_result, nonauthoritative_result)
        else:
            result = new.run()
            write_result = WriteResult(result)

        self.__client.track_migration_op(tracker)

        return write_result

    def __read_both(self, authoritative: Executor, nonauthoritative: Executor, tracker: OpTracker) -> OperationResult:
        if self.__read_execution_order == ExecutionOrder.PARALLEL:
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures.append(executor.submit(lambda: (True, authoritative.run())))
                futures.append(executor.submit(lambda: (False, nonauthoritative.run())))

                for future in concurrent.futures.as_completed(futures):
                    is_authoritative, result = future.result()
                    if is_authoritative:
                        authoritative_result = result
                    else:
                        nonauthoritative_result = result

        elif self.__read_execution_order == ExecutionOrder.RANDOM and self.__sampler.sample(2):
            nonauthoritative_result = nonauthoritative.run()
            authoritative_result = authoritative.run()
        else:
            authoritative_result = authoritative.run()
            nonauthoritative_result = nonauthoritative.run()

        if self.__read_config.comparison is None:
            return authoritative_result

        compare = self.__read_config.comparison
        if authoritative_result.is_success() and nonauthoritative_result.is_success():
            tracker.consistent(lambda: compare(authoritative_result.value, nonauthoritative_result.value))

        return authoritative_result

    def __write_both(self, authoritative: Executor, nonauthoritative: Executor, tracker: OpTracker) -> Tuple[OperationResult, Optional[OperationResult]]:
        authoritative_result = authoritative.run()
        tracker.invoked(authoritative.origin)

        if not authoritative_result.is_success():
            return authoritative_result, None

        nonauthoritative_result = nonauthoritative.run()
        tracker.invoked(nonauthoritative.origin)

        return authoritative_result, nonauthoritative_result


class MigratorBuilder:
    """
    The migration builder is used to configure and construct an instance of a
    :class:`Migrator`. This migrator can be used to perform LaunchDarkly
    assisted technology migrations through the use of migration-based feature
    flags.
    """

    def __init__(self, client: LDClient):
        # Single _ to prevent mangling; useful for testing
        self._client = client

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

        return MigratorImpl(
            Sampler(Random()),
            self._client,
            self.__read_execution_order,
            self.__read_config,
            self.__write_config,
            self.__measure_latency,
            self.__measure_errors,
        )


class Executor:
    """
    Utility class for executing migration operations while also tracking our
    built-in migration measurements.
    """

    def __init__(self, origin: Origin, fn: MigratorFn, tracker: OpTracker, measure_latency: bool, measure_errors: bool, payload: Any):
        self.__origin = origin
        self.__fn = fn
        self.__tracker = tracker
        self.__measure_latency = measure_latency
        self.__measure_errors = measure_errors
        self.__payload = payload

    @property
    def origin(self) -> Origin:
        return self.__origin

    def run(self) -> OperationResult:
        """
        Execute the configured operation and track any available measurements.
        """
        start = datetime.now()

        try:
            result = self.__fn(self.__payload)
        except Exception as e:
            result = Result.fail(f"'{self.__origin.value} operation raised an exception", e)

        # Record required tracker measurements
        if self.__measure_latency:
            self.__tracker.latency(self.__origin, datetime.now() - start)

        if self.__measure_errors and not result.is_success():
            self.__tracker.error(self.__origin)

        self.__tracker.invoked(self.__origin)

        return OperationResult(self.__origin, result)
