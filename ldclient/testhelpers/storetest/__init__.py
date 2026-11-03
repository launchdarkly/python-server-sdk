"""
Conformance test suites for persistent store implementations.

A store author extends one of these suites in a pytest test class to verify that a store follows
the rules that the SDK depends on. The suites need pytest. The async suites also need pytest-asyncio.
Neither is a dependency of the SDK, so install them in the test environment of the store.
"""

from ldclient.testhelpers.storetest.async_big_segment_store_test_suite import (
    AsyncBigSegmentStoreTestSuite
)
from ldclient.testhelpers.storetest.async_feature_store_core_test_suite import (
    AsyncFeatureStoreCoreTestSuite
)
from ldclient.testhelpers.storetest.big_segment_store_test_suite import (
    BigSegmentStoreTestSuite
)
from ldclient.testhelpers.storetest.feature_store_core_test_suite import (
    FeatureStoreCoreTestSuite
)

__all__ = [
    'AsyncBigSegmentStoreTestSuite',
    'AsyncFeatureStoreCoreTestSuite',
    'BigSegmentStoreTestSuite',
    'FeatureStoreCoreTestSuite',
]
