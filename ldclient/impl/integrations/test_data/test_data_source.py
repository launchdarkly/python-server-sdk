import copy
from ldclient.versioned_data_kind import FEATURES
from ldclient.rwlock import ReadWriteLock


class _TestDataSource():

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
