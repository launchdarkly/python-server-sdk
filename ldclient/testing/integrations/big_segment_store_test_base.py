from abc import abstractmethod, abstractproperty
from os import environ
from typing import List

import pytest

from ldclient.interfaces import BigSegmentStore, BigSegmentStoreMetadata

skip_database_tests = environ.get('LD_SKIP_DATABASE_TESTS') == '1'


# The standard test suite to be run against all Big Segment store implementations. For each database
# integration that supports Big Segments, we must define a subclass of BigSegmentStoreTester which
# overrides its abstract methods as appropriate for that database, and then define a subclass of
# BigSegmentStoreTestBase which simply specifies what tester subclass to use.

fake_user_hash = "userhash"


class BigSegmentStoreTester:
    @abstractmethod
    def create_big_segment_store(self, prefix: str) -> BigSegmentStore:
        """
        Override this method to create a Big Segment store instance.
        :param prefix: the prefix parameter for the store constructor - may be None or empty to use the default
        """
        pass

    @abstractmethod
    def clear_data(self, prefix: str):
        """
        Override this method to clear any existing data from the database for the specified prefix.
        """
        pass

    @abstractmethod
    def set_metadata(self, prefix: str, metadata: BigSegmentStoreMetadata):
        """
        Override this method to update the metadata in the store.
        """
        pass

    @abstractmethod
    def set_segments(self, prefix: str, user_hash: str, includes: List[str], excludes: List[str]):
        """
        Override this method to update segment data for a user in the store.
        """
        pass


class BigSegmentStoreTestScope:
    def __init__(self, store: BigSegmentStore):
        self.__store = store

    @property
    def store(self) -> BigSegmentStore:
        return self.__store

    # These magic methods allow the scope to be automatically cleaned up in a "with" block
    def __enter__(self):
        return self.__store

    def __exit__(self, type, value, traceback):
        self.__store.stop()


@pytest.mark.skipif(skip_database_tests, reason="skipping database tests")
class BigSegmentStoreTestBase:
    @abstractproperty
    def tester_class(self):
        pass

    @pytest.fixture(params=[False, True])
    def tester(self, request):
        specify_prefix = request.param
        instance = self.tester_class()
        instance.prefix = "testprefix" if specify_prefix else None
        return instance

    @pytest.fixture(autouse=True)
    def clear_data_before_each(self, tester):
        tester.clear_data(tester.prefix)

    def store(self, tester):
        return BigSegmentStoreTestScope(tester.create_big_segment_store(tester.prefix))

    def test_get_metadata_valid_value(self, tester):
        expected_timestamp = 1234567890
        tester.set_metadata(tester.prefix, BigSegmentStoreMetadata(expected_timestamp))
        with self.store(tester) as store:
            actual = store.get_metadata()
            assert actual is not None
            assert actual.last_up_to_date == expected_timestamp

    def test_get_metadata_no_value(self, tester):
        with self.store(tester) as store:
            actual = store.get_metadata()
            assert actual is not None
            assert actual.last_up_to_date is None

    def test_get_membership_not_found(self, tester):
        with self.store(tester) as store:
            membership = store.get_membership(fake_user_hash)
            assert membership is None or membership == {}

    def test_get_membership_includes_only(self, tester):
        tester.set_segments(tester.prefix, fake_user_hash, ['key1', 'key2'], [])
        with self.store(tester) as store:
            membership = store.get_membership(fake_user_hash)
            assert membership == {'key1': True, 'key2': True}

    def test_get_membership_excludes_only(self, tester):
        tester.set_segments(tester.prefix, fake_user_hash, [], ['key1', 'key2'])
        with self.store(tester) as store:
            membership = store.get_membership(fake_user_hash)
            assert membership == {'key1': False, 'key2': False}

    def test_get_membership_includes_and_excludes(self, tester):
        tester.set_segments(tester.prefix, fake_user_hash, ['key1', 'key2'], ['key2', 'key3'])
        with self.store(tester) as store:
            membership = store.get_membership(fake_user_hash)
            assert membership == {'key1': True, 'key2': True, 'key3': False}
