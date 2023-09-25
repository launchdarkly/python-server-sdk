from ldclient import LDClient, Context
from ldclient.migrations import ExecutionOrder, OperationResult, Stage, Migrator as IMigrator, MigrationConfig
from typing import Any, Optional


class Migrator(IMigrator):
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
