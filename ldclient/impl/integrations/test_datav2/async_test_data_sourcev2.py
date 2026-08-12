import asyncio
from typing import AsyncGenerator

from ldclient.impl.util import _Fail, _Success, current_time_millis
from ldclient.interfaces import (
    Basis,
    BasisResult,
    ChangeSetBuilder,
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    IntentCode,
    ObjectKind,
    Selector,
    SelectorStore,
    Update
)


class _AsyncTestDataSourceV2:
    """
    Async implementation of both the Initializer and Synchronizer protocols for TestDataV2.

    The async twin of :class:`_TestDataSourceV2`: it shares the same TestDataV2 flag
    management but exposes ``async`` ``fetch``/``sync``/``stop`` and delivers updates
    through an :class:`asyncio.Queue` so it can drive the async data system.
    """

    def __init__(self, test_data):
        self._test_data = test_data
        self._closed = False
        self._update_queue: asyncio.Queue = asyncio.Queue()

        # Register for change notifications; upsert_flag is invoked on updates.
        self._test_data._add_instance(self)

    @property
    def name(self) -> str:
        """Return the name of this data source."""
        return "TestDataV2"

    async def fetch(self, ss: SelectorStore) -> BasisResult:
        """Implementation of the AsyncInitializer.fetch method."""
        return self._make_basis()

    async def sync(self, ss: SelectorStore) -> AsyncGenerator[Update, None]:
        """Implementation of the AsyncSynchronizer.sync method: yields the initial
        data, then each update as it is queued, until the source is stopped."""
        initial_result = self._make_basis()
        if isinstance(initial_result, _Fail):
            yield Update(
                state=DataSourceState.OFF,
                error=DataSourceErrorInfo(
                    kind=DataSourceErrorKind.STORE_ERROR,
                    status_code=0,
                    time=current_time_millis(),
                    message=initial_result.error,
                ),
            )
            return

        yield Update(
            state=DataSourceState.VALID, change_set=initial_result.value.change_set
        )

        while not self._closed:
            update = await self._update_queue.get()
            if update is None:  # Sentinel value for shutdown
                break
            yield update

    async def stop(self):
        """Stop the data source and clean up resources."""
        if self._closed:
            return
        self._closed = True
        self._test_data._closed_instance(self)
        # Wake the sync generator so it can exit.
        self._update_queue.put_nowait(None)

    def upsert_flag(self, flag_data: dict):
        """Called by TestDataV2 when a flag is updated; queues the change for
        delivery through the sync() generator."""
        if self._closed:
            return
        try:
            version = self._test_data._get_version()

            builder = ChangeSetBuilder()
            builder.start(IntentCode.TRANSFER_CHANGES)
            builder.add_put(
                ObjectKind.FLAG,
                flag_data["key"],
                flag_data.get("version", 1),
                flag_data,
            )

            selector = Selector.new_selector(str(version), version)
            change_set = builder.finish(selector)

            self._update_queue.put_nowait(
                Update(state=DataSourceState.VALID, change_set=change_set)
            )
        except Exception as e:
            self._update_queue.put_nowait(
                Update(
                    state=DataSourceState.OFF,
                    error=DataSourceErrorInfo(
                        kind=DataSourceErrorKind.STORE_ERROR,
                        status_code=0,
                        time=current_time_millis(),
                        message=f"Error processing flag update: {str(e)}",
                    ),
                )
            )

    def _make_basis(self) -> BasisResult:
        """Builds a full-transfer Basis from the current test data. Shared by
        fetch() and the initial yield of sync()."""
        try:
            if self._closed:
                return _Fail("TestDataV2 source has been closed")

            init_data = self._test_data._make_init_data()
            version = self._test_data._get_version()

            builder = ChangeSetBuilder()
            builder.start(IntentCode.TRANSFER_FULL)
            for key, flag_data in init_data.items():
                builder.add_put(
                    ObjectKind.FLAG, key, flag_data.get("version", 1), flag_data
                )

            selector = Selector.new_selector(str(version), version)
            change_set = builder.finish(selector)
            basis = Basis(change_set=change_set, persist=False, environment_id=None)

            return _Success(basis)
        except Exception as e:
            return _Fail(f"Error fetching test data: {str(e)}")
