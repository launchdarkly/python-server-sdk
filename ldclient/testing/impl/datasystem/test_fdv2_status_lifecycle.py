"""
The FDv2 data system reports OFF only as its last status. A synchronizer that reports OFF has
failed, but the data system goes on with the next synchronizer, so it reports INTERRUPTED. The
data system reports OFF when it stops, or when it has no synchronizer left.
"""

import asyncio
from threading import Event
from typing import AsyncGenerator, List

import pytest
from mock import Mock

from ldclient.async_config import AsyncConfig, AsyncDataSystemConfig
from ldclient.config import Config, DataSourceBuilder, DataSystemConfig
from ldclient.impl.datasystem.async_fdv2 import AsyncFDv2
from ldclient.impl.datasystem.fdv2 import FDv2
from ldclient.integrations.test_datav2 import TestDataV2
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataSourceStatus,
    SelectorStore,
    Update
)

ERROR = DataSourceErrorInfo(DataSourceErrorKind.ERROR_RESPONSE, 401, 0, 'unauthorized')


class _Builder(DataSourceBuilder):
    def __init__(self, source):
        self._source = source

    def build(self, config):
        return self._source


class _ListAsyncSynchronizer:
    """An async synchronizer that yields a fixed list of updates and then completes."""

    def __init__(self, updates: List[Update]):
        self._updates = updates

    @property
    def name(self) -> str:
        return 'list-synchronizer'

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        for update in self._updates:
            yield update

    async def stop(self) -> None:
        pass


class _StateRecorder:
    def __init__(self):
        self.states: List[DataSourceState] = []
        self.valid_after_interrupted = Event()

    def __call__(self, status: DataSourceStatus):
        self.states.append(status.state)
        if status.state == DataSourceState.VALID and DataSourceState.INTERRUPTED in self.states:
            self.valid_after_interrupted.set()


def _failing_synchronizer() -> Mock:
    synchronizer = Mock()
    synchronizer.name = 'failing-synchronizer'
    synchronizer.sync.return_value = iter([Update(state=DataSourceState.VALID), Update(state=DataSourceState.OFF, error=ERROR)])
    return synchronizer


def _test_data() -> TestDataV2:
    td = TestDataV2.data_source()
    td.update(td.flag('flag').on(True))
    return td


def test_synchronizer_off_is_reported_as_interrupted_before_the_next_synchronizer():
    td = _test_data()
    fdv2 = FDv2(Config('sdk-key'), DataSystemConfig(initializers=None, synchronizers=[_Builder(_failing_synchronizer()), td.builder]))
    recorder = _StateRecorder()
    fdv2.data_source_status_provider.add_listener(recorder)

    fdv2.start(Event())
    try:
        assert recorder.valid_after_interrupted.wait(2)
        assert DataSourceState.OFF not in recorder.states
        assert fdv2.data_source_status_provider.status.error == ERROR
    finally:
        fdv2.stop()


def test_stop_reports_off():
    td = _test_data()
    fdv2 = FDv2(Config('sdk-key'), DataSystemConfig(initializers=None, synchronizers=[td.builder]))
    ready = Event()
    fdv2.start(ready)
    assert ready.wait(2)
    assert fdv2.data_source_status_provider.status.state == DataSourceState.VALID

    fdv2.stop()

    assert fdv2.data_source_status_provider.status.state == DataSourceState.OFF


@pytest.mark.asyncio
async def test_async_synchronizer_off_is_reported_as_interrupted_before_the_next_synchronizer():
    td = _test_data()
    failing = _ListAsyncSynchronizer([Update(state=DataSourceState.VALID), Update(state=DataSourceState.OFF, error=ERROR)])
    fdv2 = AsyncFDv2(AsyncConfig('sdk-key'), AsyncDataSystemConfig(synchronizers=[_Builder(failing), td.async_builder]))
    recorder = _StateRecorder()
    fdv2.data_source_status_provider.add_listener(recorder)

    fdv2.start(asyncio.Event())
    try:
        for _ in range(200):
            if recorder.valid_after_interrupted.is_set():
                break
            await asyncio.sleep(0.01)
        assert recorder.valid_after_interrupted.is_set()
        assert DataSourceState.OFF not in recorder.states
        assert fdv2.data_source_status_provider.status.error == ERROR
    finally:
        await fdv2.stop()


@pytest.mark.asyncio
async def test_async_stop_reports_off():
    td = _test_data()
    fdv2 = AsyncFDv2(AsyncConfig('sdk-key'), AsyncDataSystemConfig(synchronizers=[td.async_builder]))
    ready = asyncio.Event()
    fdv2.start(ready)
    await asyncio.wait_for(ready.wait(), timeout=2)
    assert fdv2.data_source_status_provider.status.state == DataSourceState.VALID

    await fdv2.stop()

    assert fdv2.data_source_status_provider.status.state == DataSourceState.OFF
