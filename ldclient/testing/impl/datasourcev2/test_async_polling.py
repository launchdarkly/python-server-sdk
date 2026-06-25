# pylint: disable=missing-docstring

import asyncio
import time
from typing import List, Optional

import pytest

from ldclient.impl.datasourcev2.async_polling import (
    AsyncPollingDataSource,
    PollingResult
)
from ldclient.impl.util import (
    _LD_ENVID_HEADER,
    _LD_FD_FALLBACK_HEADER,
    UnsuccessfulResponseException,
    _Fail,
    _Success
)
from ldclient.interfaces import (
    ChangeSetBuilder,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.testing.mock_components import MockSelectorStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class MockPollingRequester:  # pylint: disable=too-few-public-methods
    def __init__(self, results: List[PollingResult]):
        self._results = list(results)
        self._index = 0
        self.call_times: List[float] = []

    async def fetch(self, selector: Optional[Selector]) -> PollingResult:
        self.call_times.append(time.monotonic())
        result = self._results[self._index % len(self._results)]
        self._index += 1
        return result


class MockExceptionThrowingRequester:  # pylint: disable=too-few-public-methods
    async def fetch(self, selector: Optional[Selector]) -> PollingResult:
        raise RuntimeError("requester blew up")


def _valid_change_set():
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    builder.add_put(ObjectKind.FLAG, "flag-key", 100, {"key": "flag-key"})
    return builder.finish(Selector(state="p:SOMETHING:300", version=300))


def _make_source(results: List[PollingResult], poll_interval: float = 0.01) -> AsyncPollingDataSource:
    return AsyncPollingDataSource(
        poll_interval=poll_interval,
        requester=MockPollingRequester(results),
    )


def _ss() -> MockSelectorStore:
    return MockSelectorStore(Selector.no_selector())


# ---------------------------------------------------------------------------
# fetch() (initializer)
# ---------------------------------------------------------------------------


def test_name():
    src = _make_source([_Fail(error="failure message")])
    assert src.name == "PollingDataSourceV2"


@pytest.mark.asyncio
async def test_fetch_success():
    change_set = _valid_change_set()
    src = _make_source([_Success(value=(change_set, {_LD_ENVID_HEADER: "env1"}))])

    result = await src.fetch(_ss())
    assert isinstance(result, _Success)
    basis = result.value
    assert basis.change_set is change_set
    assert basis.persist is True
    assert basis.environment_id == "env1"
    assert basis.fallback_to_fdv1 is False


@pytest.mark.asyncio
async def test_fetch_failure_passes_through():
    src = _make_source([_Fail(error="failure message")])

    result = await src.fetch(_ss())
    assert isinstance(result, _Fail)
    assert result.error == "failure message"


@pytest.mark.asyncio
async def test_fetch_recoverable_error():
    src = _make_source([_Fail(error="500", exception=UnsuccessfulResponseException(500))])

    result = await src.fetch(_ss())
    assert isinstance(result, _Fail)
    assert result.error.startswith("Received HTTP error 500")


@pytest.mark.asyncio
async def test_fetch_unrecoverable_error():
    src = _make_source([_Fail(error="401", exception=UnsuccessfulResponseException(401))])

    result = await src.fetch(_ss())
    assert isinstance(result, _Fail)
    assert result.error.startswith("Received HTTP error 401")


@pytest.mark.asyncio
async def test_fetch_no_changes():
    src = _make_source([_Success(value=(ChangeSetBuilder.no_changes(), {}))])

    result = await src.fetch(_ss())
    assert isinstance(result, _Success)
    basis = result.value
    assert basis.persist is False
    assert not basis.change_set.selector.is_defined()


@pytest.mark.asyncio
async def test_fetch_requester_exception_is_caught():
    src = AsyncPollingDataSource(poll_interval=0.01, requester=MockExceptionThrowingRequester())

    result = await src.fetch(_ss())
    assert isinstance(result, _Fail)
    assert "Exception encountered when updating flags" in result.error


# ---------------------------------------------------------------------------
# sync() (synchronizer)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_yields_valid_update():
    change_set = _valid_change_set()
    src = _make_source([_Success(value=(change_set, {_LD_ENVID_HEADER: "env1"}))], poll_interval=60)

    gen = src.sync(_ss())
    update = await gen.__anext__()
    await gen.aclose()

    assert update.state == DataSourceState.VALID
    assert update.change_set is change_set
    assert update.environment_id == "env1"
    assert update.fallback_to_fdv1 is False


@pytest.mark.asyncio
async def test_sync_yields_interrupted_on_recoverable_error():
    src = _make_source([_Fail(error="500", exception=UnsuccessfulResponseException(500))], poll_interval=60)

    gen = src.sync(_ss())
    update = await gen.__anext__()
    await gen.aclose()

    assert update.state == DataSourceState.INTERRUPTED
    assert update.error is not None
    assert update.error.kind == DataSourceErrorKind.ERROR_RESPONSE
    assert update.error.status_code == 500


@pytest.mark.asyncio
async def test_sync_yields_off_on_unrecoverable_error():
    src = _make_source([_Fail(error="401", exception=UnsuccessfulResponseException(401))], poll_interval=60)

    updates = [update async for update in src.sync(_ss())]

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF


@pytest.mark.asyncio
async def test_sync_network_error_yields_interrupted():
    call_count = 0

    class _StoppingRequester:
        async def fetch(self, selector):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                await src.stop()
            return _Fail(error="connection refused")

    src = AsyncPollingDataSource(poll_interval=0.01, requester=_StoppingRequester())

    updates = [update async for update in src.sync(_ss())]

    assert len(updates) >= 1
    assert updates[0].state == DataSourceState.INTERRUPTED
    assert updates[0].error.kind == DataSourceErrorKind.NETWORK_ERROR


@pytest.mark.asyncio
async def test_sync_fallback_to_fdv1_on_error():
    headers = {_LD_FD_FALLBACK_HEADER: 'true', _LD_ENVID_HEADER: 'env1'}
    src = _make_source(
        [_Fail(error="403", exception=UnsuccessfulResponseException(403), headers=headers)],
        poll_interval=60,
    )

    updates = [update async for update in src.sync(_ss())]

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.OFF
    assert updates[0].fallback_to_fdv1 is True
    assert updates[0].environment_id == 'env1'


@pytest.mark.asyncio
async def test_sync_fallback_to_fdv1_on_success():
    change_set = _valid_change_set()
    headers = {_LD_FD_FALLBACK_HEADER: 'true'}
    src = _make_source([_Success(value=(change_set, headers))], poll_interval=60)

    gen = src.sync(_ss())
    update = await gen.__anext__()
    await gen.aclose()

    assert update.state == DataSourceState.VALID
    assert update.fallback_to_fdv1 is True


@pytest.mark.asyncio
async def test_sync_interval_is_respected():
    requester = MockPollingRequester([_Success(value=(_valid_change_set(), {}))])
    src = AsyncPollingDataSource(poll_interval=0.1, requester=requester)

    updates = []
    async for update in src.sync(_ss()):
        updates.append(update)
        if len(updates) >= 2:
            await src.stop()

    assert len(requester.call_times) >= 2
    elapsed = requester.call_times[1] - requester.call_times[0]
    assert elapsed >= 0.05, f"Poll interval too short: {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_stop_halts_sync():
    src = _make_source([_Success(value=(_valid_change_set(), {}))], poll_interval=60)

    updates = []
    first_update_received = asyncio.Event()

    async def consume():
        async for update in src.sync(_ss()):
            updates.append(update)
            first_update_received.set()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(first_update_received.wait(), timeout=2)
    await src.stop()
    await asyncio.wait_for(task, timeout=2)

    assert len(updates) == 1
    assert updates[0].state == DataSourceState.VALID
