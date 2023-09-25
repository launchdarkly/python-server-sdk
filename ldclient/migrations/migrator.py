from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Optional, Union, Any, TYPE_CHECKING
from ldclient.migrations.types import ExecutionOrder, OperationResult, Stage, MigrationConfig, MigratorFn, MigratorCompareFn

if TYPE_CHECKING:
    from ldclient import LDClient, Context


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


class MigratorImpl(Migrator):
    """
    An implementation of :class:`ldclient.migrations.Migrator` interface,
    capable of supporting feature-flag backed technology migrations.
    """

    def __init__(
        self,
        client: LDClient,
        read_execution_order: ExecutionOrder,
        read_config: MigrationConfig,
        write_config: MigrationConfig,
        measure_latency: bool,
        measure_errors: bool
    ):
        self.__client = client
        self.__read_execution_order = read_execution_order
        self.__read_config = read_config
        self.__write_config = write_config
        self.__measure_latency = measure_latency
        self.__measure_errors = measure_errors

    def read(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        # TODO: Implement this in a subsequent ticket
        pass

    def write(self, key: str, context: Context, default_stage: Stage, payload: Optional[Any] = None) -> OperationResult:
        # TODO: Implement this in a subsequent ticket
        pass


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

        return MigratorImpl(
            self.__client,
            self.__read_execution_order,
            self.__read_config,
            self.__write_config,
            self.__measure_latency,
            self.__measure_errors,
        )
