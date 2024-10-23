import pytest

from ldclient import Result
from ldclient.client import Config, LDClient
from ldclient.migrations import ExecutionOrder, Migrator, MigratorBuilder


def test_can_build_successfully():
    client = LDClient(config=Config(sdk_key='SDK_KEY'))
    builder = MigratorBuilder(client)
    builder.read(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
        None,
    )
    builder.write(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
    )
    migrator = builder.build()

    assert isinstance(migrator, Migrator)


@pytest.mark.parametrize(
    "order",
    [
        pytest.param(ExecutionOrder.SERIAL, id="serial"),
        pytest.param(ExecutionOrder.RANDOM, id="random"),
        pytest.param(ExecutionOrder.PARALLEL, id="parallel"),
    ],
)
def test_can_modify_execution_order(order):
    client = LDClient(config=Config(sdk_key='SDK_KEY'))
    builder = MigratorBuilder(client)
    builder.read(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
        None,
    )
    builder.write(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
    )
    builder.read_execution_order(order)
    migrator = builder.build()

    assert isinstance(migrator, Migrator)


def test_build_fails_without_read():
    client = LDClient(config=Config(sdk_key='SDK_KEY'))
    builder = MigratorBuilder(client)
    builder.write(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
    )
    migrator = builder.build()

    assert isinstance(migrator, str)
    assert migrator == "read configuration not provided"


def test_build_fails_without_write():
    client = LDClient(config=Config(sdk_key='SDK_KEY'))
    builder = MigratorBuilder(client)
    builder.read(
        lambda payload: Result.success("old origin"),
        lambda payload: Result.success("new origin"),
    )
    migrator = builder.build()

    assert isinstance(migrator, str)
    assert migrator == "write configuration not provided"
