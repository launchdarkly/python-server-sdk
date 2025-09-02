import threading
from queue import Empty, Queue
from typing import Generator

from ldclient.impl.datasystem import BasisResult, Update
from ldclient.impl.datasystem.protocolv2 import (
    Basis,
    ChangeSetBuilder,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.impl.util import _Fail, _Success, current_time_millis
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState
)


class _TestDataSourceV2:
    """
    Internal implementation of both Initializer and Synchronizer protocols for TestDataV2.

    This component bridges the test data management in TestDataV2 with the FDv2 protocol
    interfaces. Each instance implements both Initializer and Synchronizer protocols
    and receives change notifications for dynamic updates.
    """

    def __init__(self, test_data):
        self._test_data = test_data
        self._closed = False
        self._update_queue = Queue()
        self._lock = threading.Lock()

        # Always register for change notifications
        self._test_data._add_instance(self)

        # Locking strategy:
        # The threading.Lock instance (_lock) ensures thread safety for shared resources:
        # - Used in `fetch` and `close` to prevent concurrent modification of `_closed`.
        # - Added to `upsert_flag` to address potential race conditions.
        # - The `sync` method relies on Queue's thread-safe properties for updates.

    def fetch(self) -> BasisResult:
        """
        Implementation of the Initializer.fetch method.

        Returns the current test data as a Basis for initial data loading.
        """
        try:
            with self._lock:
                if self._closed:
                    return _Fail("TestDataV2 source has been closed")

                # Get all current flags from test data
                init_data = self._test_data._make_init_data()
                version = self._test_data._get_version()

                # Build a full transfer changeset
                builder = ChangeSetBuilder()
                builder.start(IntentCode.TRANSFER_FULL)

                # Add all flags to the changeset
                for key, flag_data in init_data.items():
                    builder.add_put(
                        ObjectKind.FLAG,
                        key,
                        flag_data.get('version', 1),
                        flag_data
                    )

                # Create selector for this version
                selector = Selector.new_selector(str(version), version)
                change_set = builder.finish(selector)

                basis = Basis(
                    change_set=change_set,
                    persist=False,
                    environment_id=None
                )

                return _Success(basis)

        except Exception as e:
            return _Fail(f"Error fetching test data: {str(e)}")

    def sync(self) -> Generator[Update, None, None]:
        """
        Implementation of the Synchronizer.sync method.

        Yields updates as test data changes occur.
        """

        # First yield initial data
        initial_result = self.fetch()
        if isinstance(initial_result, _Fail):
            yield Update(
                state=DataSourceState.OFF,
                error=DataSourceErrorInfo(
                    kind=DataSourceErrorKind.STORE_ERROR,
                    status_code=0,
                    time=current_time_millis(),
                    message=initial_result.error
                )
            )
            return

        # Yield the initial successful state
        yield Update(
            state=DataSourceState.VALID,
            change_set=initial_result.value.change_set
        )

        # Continue yielding updates as they arrive
        while not self._closed:
            try:
                # Wait for updates with a timeout to allow checking closed status
                try:
                    update = self._update_queue.get(timeout=1.0)
                except Empty:
                    continue

                if update is None:  # Sentinel value for shutdown
                    break

                yield update

            except Exception as e:
                yield Update(
                    state=DataSourceState.OFF,
                    error=DataSourceErrorInfo(
                        kind=DataSourceErrorKind.UNKNOWN,
                        status_code=0,
                        time=current_time_millis(),
                        message=f"Error in test data synchronizer: {str(e)}"
                    )
                )
                break

    def close(self):
        """Close the data source and clean up resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        self._test_data._closed_instance(self)
        # Signal shutdown to sync generator
        self._update_queue.put(None)

    def upsert_flag(self, flag_data: dict):
        """
        Called by TestDataV2 when a flag is updated.

        This method converts the flag update into an FDv2 changeset and
        queues it for delivery through the sync() generator.
        """
        with self._lock:
            if self._closed:
                return

            try:
                version = self._test_data._get_version()

                # Build a changes transfer changeset
                builder = ChangeSetBuilder()
                builder.start(IntentCode.TRANSFER_CHANGES)

                # Add the updated flag
                builder.add_put(
                    ObjectKind.FLAG,
                    flag_data['key'],
                    flag_data.get('version', 1),
                    flag_data
                )

                # Create selector for this version
                selector = Selector.new_selector(str(version), version)
                change_set = builder.finish(selector)

                # Queue the update
                update = Update(
                    state=DataSourceState.VALID,
                    change_set=change_set
                )

                self._update_queue.put(update)

            except Exception as e:
                # Queue an error update
                error_update = Update(
                    state=DataSourceState.OFF,
                    error=DataSourceErrorInfo(
                        kind=DataSourceErrorKind.STORE_ERROR,
                        status_code=0,
                        time=current_time_millis(),
                        message=f"Error processing flag update: {str(e)}"
                    )
                )
                self._update_queue.put(error_update)
