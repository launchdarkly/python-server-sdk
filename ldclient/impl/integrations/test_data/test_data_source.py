from ldclient.interfaces import UpdateProcessor
from ldclient.versioned_data_kind import FEATURES

# This is the internal component that's created when you initialize an SDK instance that is using
# TestData. The TestData object manages the setup of the fake data, and it broadcasts the data
# through _TestDataSource to inject it into the SDK. If there are multiple SDK instances connected
# to a TestData, each has its own _TestDataSource.


class _TestDataSource(UpdateProcessor):

    def __init__(self, feature_store, test_data, ready):
        self._feature_store = feature_store
        self._test_data = test_data
        self._ready = ready

    def start(self):
        self._ready.set()
        self._feature_store.init(self._test_data._make_init_data())

    def stop(self):
        self._test_data._closed_instance(self)

    def initialized(self):
        return True

    def upsert(self, new_flag):
        self._feature_store.upsert(FEATURES, new_flag)
