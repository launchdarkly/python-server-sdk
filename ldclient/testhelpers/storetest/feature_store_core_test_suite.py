from abc import ABC, abstractmethod
from contextlib import closing, contextmanager
from typing import Callable, Iterator, Optional

import pytest

from ldclient.interfaces import FeatureStoreCore
from ldclient.testhelpers.storetest._items import (
    KIND,
    OTHER_KIND,
    assert_deleted,
    assert_item,
    data_set,
    deleted_item,
    item,
    keyless_deleted_item
)


class FeatureStoreCoreTestSuite(ABC):
    """
    A conformance test suite for implementations of :class:`ldclient.interfaces.FeatureStoreCore`.

    The suite runs with pytest. To use it, define a test class that extends this class, and
    implement :func:`create_core` and :func:`clear_data`::

        class TestMyStoreCore(FeatureStoreCoreTestSuite):
            def create_core(self, prefix):
                return MyStoreCore(prefix=prefix)

            def clear_data(self, prefix):
                ...

    The store must have these characteristics:

    1. A prefix string separates the data of different SDK instances that use the same database.
    2. Two cores of the same type, with the same configuration and the same prefix, see the same
       data.

    The other methods enable optional tests. If a subclass does not override one of them, pytest
    skips the tests that need it.

    The suite tests the core directly, without
    :class:`ldclient.feature_store_helpers.CachingStoreWrapper`. Each item is an opaque string to
    the core. A core can store the version and the deleted state separately, or only in the
    serialized string, and the suite accepts both.
    """

    @abstractmethod
    def create_core(self, prefix: Optional[str]) -> FeatureStoreCore:
        """
        Returns a new core that uses the given prefix.

        :param prefix: the prefix for the store data, or None to use the default prefix of the store
        """

    @abstractmethod
    def clear_data(self, prefix: Optional[str]) -> None:
        """
        Deletes all data in the database for the given prefix.

        :param prefix: the prefix for the store data, or None to use the default prefix of the store
        """

    def create_error_core(self) -> FeatureStoreCore:
        """
        Returns a core that cannot reach its database, so that each operation fails. Override this
        method to enable the error tests.
        """
        pytest.skip("the store does not supply a core that fails")

    def set_concurrent_modification_hook(self, core: FeatureStoreCore, hook: Callable[[], None]) -> None:
        """
        Makes the core call ``hook`` during each upsert, after the core reads the existing item and
        before it writes the new item. Override this method to enable the concurrent modification
        tests.

        :param core: a core that :func:`create_core` returned
        :param hook: the function to call
        """
        pytest.skip("the store does not support a concurrent modification hook")

    @contextmanager
    def _core(self, prefix: Optional[str] = None) -> Iterator[FeatureStoreCore]:
        core = self.create_core(prefix)
        try:
            yield core
        finally:
            core.close()

    @contextmanager
    def _inited_core(self) -> Iterator[FeatureStoreCore]:
        self.clear_data(None)
        with self._core() as core:
            core.init_internal(data_set())
            yield core

    def test_initialized_after_init(self):
        self.clear_data(None)
        with self._core() as core:
            core.init_internal(data_set(item('flag', 1)))
            assert core.initialized_internal()

    def test_init_replaces_previous_data(self):
        self.clear_data(None)
        with self._core() as core:
            first, second = item('first', 1), item('second', 1)
            other_first = item('first', 1)
            core.init_internal(data_set(first, second, other={'first': other_first}))

            items = core.get_all_internal(KIND)
            assert len(items) == 2
            assert_item(first, items.get('first'))
            assert_item(second, items.get('second'))

            other_items = core.get_all_internal(OTHER_KIND)
            assert len(other_items) == 1
            assert_item(other_first, other_items.get('first'))

            other_second = item('second', 1)
            core.init_internal(data_set(first, other={'second': other_second}))

            items = core.get_all_internal(KIND)
            assert len(items) == 1
            assert_item(first, items.get('first'))

            other_items = core.get_all_internal(OTHER_KIND)
            assert len(other_items) == 1
            assert_item(other_second, other_items.get('second'))

    def test_one_core_detects_init_by_another(self):
        self.clear_data(None)
        with self._core() as core1:
            with self._core() as core2:
                assert not core1.initialized_internal()

                core2.init_internal(data_set())

                assert core1.initialized_internal()

    def test_get_existing_item(self):
        with self._inited_core() as core:
            flag = item('flag', 1)
            assert core.upsert_internal(KIND, 'flag', flag) is True

            assert_item(flag, core.get_internal(KIND, 'flag'))

    def test_get_nonexistent_item(self):
        with self._inited_core() as core:
            assert core.get_internal(KIND, 'no') is None

    def test_get_all_items(self):
        with self._inited_core() as core:
            assert core.get_all_internal(KIND) == {}

            first, second = item('first', 1), item('second', 1)
            core.upsert_internal(KIND, 'first', first)
            core.upsert_internal(KIND, 'second', second)
            core.upsert_internal(OTHER_KIND, 'first', item('first', 1))

            items = core.get_all_internal(KIND)
            assert len(items) == 2
            assert_item(first, items.get('first'))
            assert_item(second, items.get('second'))

    def test_get_all_reads_an_empty_kind_from_the_database(self):
        # Some database clients report "nothing matched" with a null value rather than an empty
        # result. A second core reads the database, not data that the first core holds.
        self.clear_data(None)
        with self._core() as core1:
            core1.init_internal(data_set())
            with self._core() as core2:
                assert core2.get_all_internal(KIND) == {}

    def test_get_all_keys_items_by_the_key_they_are_stored_under(self):
        # Other LaunchDarkly SDKs write a deleted item with only a version, and no key of its own.
        with self._inited_core() as core:
            core.upsert_internal(KIND, 'flag', keyless_deleted_item(5))

            items = core.get_all_internal(KIND)
            assert list(items.keys()) == ['flag']
            assert_deleted(5, items['flag'])

    def test_upsert_newer_version(self):
        with self._inited_core() as core:
            core.upsert_internal(KIND, 'flag', item('flag', 10, 'original'))

            updated = item('flag', 11, 'updated')
            assert core.upsert_internal(KIND, 'flag', updated) is True

            assert_item(updated, core.get_internal(KIND, 'flag'))

    def test_upsert_older_version(self):
        with self._inited_core() as core:
            original = item('flag', 10, 'original')
            core.upsert_internal(KIND, 'flag', original)

            assert core.upsert_internal(KIND, 'flag', item('flag', 9, 'updated')) is False

            assert_item(original, core.get_internal(KIND, 'flag'))

    def test_upsert_same_version(self):
        with self._inited_core() as core:
            original = item('flag', 10, 'original')
            core.upsert_internal(KIND, 'flag', original)

            assert core.upsert_internal(KIND, 'flag', item('flag', 10, 'updated')) is False

            assert_item(original, core.get_internal(KIND, 'flag'))

    def test_delete_with_newer_version(self):
        with self._inited_core() as core:
            core.upsert_internal(KIND, 'flag', item('flag', 10))

            assert core.upsert_internal(KIND, 'flag', deleted_item('flag', 11)) is True

            assert_deleted(11, core.get_internal(KIND, 'flag'))

    def test_delete_with_older_version(self):
        with self._inited_core() as core:
            original = item('flag', 10)
            core.upsert_internal(KIND, 'flag', original)

            assert core.upsert_internal(KIND, 'flag', deleted_item('flag', 9)) is False

            assert_item(original, core.get_internal(KIND, 'flag'))

    def test_delete_with_same_version(self):
        with self._inited_core() as core:
            original = item('flag', 10)
            core.upsert_internal(KIND, 'flag', original)

            assert core.upsert_internal(KIND, 'flag', deleted_item('flag', 10)) is False

            assert_item(original, core.get_internal(KIND, 'flag'))

    def test_delete_unknown_item(self):
        with self._inited_core() as core:
            assert core.upsert_internal(KIND, 'flag', deleted_item('flag', 1)) is True

            assert_deleted(1, core.get_internal(KIND, 'flag'))

    def test_upsert_older_version_after_delete(self):
        with self._inited_core() as core:
            original = item('flag', 10)
            core.upsert_internal(KIND, 'flag', original)
            core.upsert_internal(KIND, 'flag', deleted_item('flag', 11))

            assert core.upsert_internal(KIND, 'flag', original) is False

            assert_deleted(11, core.get_internal(KIND, 'flag'))

    def test_is_available(self):
        with self._core() as core:
            assert core.is_available() is True

    def test_close_twice(self):
        core = self.create_core(None)
        core.close()
        core.close()

    def test_prefixes_are_independent_for_init(self):
        self.clear_data('testprefix1')
        self.clear_data('testprefix2')
        with self._core('testprefix1') as core1:
            with self._core('testprefix2') as core2:
                assert not core1.initialized_internal()
                assert not core2.initialized_internal()

                item1a, item1b = item('flag-a', 1), item('flag-b', 1)
                item2a, item2c = item('flag-a', 2), item('flag-c', 2)

                core1.init_internal(data_set(item1a, item1b))
                assert core1.initialized_internal()
                assert not core2.initialized_internal()

                core2.init_internal(data_set(item2a, item2c))
                assert core1.initialized_internal()
                assert core2.initialized_internal()

                items1 = core1.get_all_internal(KIND)
                assert len(items1) == 2
                assert_item(item1a, items1.get('flag-a'))
                assert_item(item1b, items1.get('flag-b'))
                assert_item(item1a, core1.get_internal(KIND, 'flag-a'))
                assert_item(item1b, core1.get_internal(KIND, 'flag-b'))

                items2 = core2.get_all_internal(KIND)
                assert len(items2) == 2
                assert_item(item2a, items2.get('flag-a'))
                assert_item(item2c, items2.get('flag-c'))
                assert_item(item2a, core2.get_internal(KIND, 'flag-a'))
                assert_item(item2c, core2.get_internal(KIND, 'flag-c'))

    def test_prefixes_are_independent_for_upsert(self):
        self.clear_data('testprefix1')
        self.clear_data('testprefix2')
        with self._core('testprefix1') as core1:
            with self._core('testprefix2') as core2:
                version1, version2 = item('flag', 1), item('flag', 2)

                # Write the higher version first. The version check of the other core must then
                # look only at the data for its own prefix.
                assert core2.upsert_internal(KIND, 'flag', version2) is True
                assert core1.upsert_internal(KIND, 'flag', version1) is True

                assert_item(version1, core1.get_internal(KIND, 'flag'))
                assert_item(version2, core2.get_internal(KIND, 'flag'))

                assert core1.upsert_internal(KIND, 'flag', version2) is True
                assert_item(version2, core1.get_internal(KIND, 'flag'))

    def test_errors_from_init(self):
        with closing(self.create_error_core()) as core:
            with pytest.raises(Exception):
                core.init_internal(data_set())

    def test_errors_from_get(self):
        with closing(self.create_error_core()) as core:
            with pytest.raises(Exception):
                core.get_internal(KIND, 'flag')

    def test_errors_from_get_all(self):
        with closing(self.create_error_core()) as core:
            with pytest.raises(Exception):
                core.get_all_internal(KIND)

    def test_errors_from_upsert(self):
        with closing(self.create_error_core()) as core:
            with pytest.raises(Exception):
                core.upsert_internal(KIND, 'flag', item('flag', 1))

    def test_error_core_is_not_available(self):
        with closing(self.create_error_core()) as core:
            assert core.is_available() is False

    def test_upsert_race_against_a_lower_version(self):
        # Another writer changes the item while the core upserts. The writes of the other writer
        # have lower versions, so the upsert of the core must win.
        self.clear_data(None)
        with self._core() as core1:
            with self._core() as core2:
                core1.init_internal(data_set(item('flag', 1)))
                self.set_concurrent_modification_hook(core1, _writer(core2, [2, 3, 4]))

                core1.upsert_internal(KIND, 'flag', item('flag', 10))

                assert_item(item('flag', 10), core1.get_internal(KIND, 'flag'))

    def test_upsert_race_against_a_higher_version(self):
        # Another writer writes a higher version while the core upserts. The upsert of the core
        # must then fail.
        self.clear_data(None)
        with self._core() as core1:
            with self._core() as core2:
                core1.init_internal(data_set(item('flag', 1)))
                self.set_concurrent_modification_hook(core1, _writer(core2, [3]))

                assert core1.upsert_internal(KIND, 'flag', item('flag', 2)) is False

                assert_item(item('flag', 3), core1.get_internal(KIND, 'flag'))


def _writer(core: FeatureStoreCore, versions) -> Callable[[], None]:
    remaining = list(versions)

    def write():
        if remaining:
            core.upsert_internal(KIND, 'flag', item('flag', remaining.pop(0)))

    return write
