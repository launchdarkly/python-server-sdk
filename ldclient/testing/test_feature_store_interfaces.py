import pytest

from ldclient.interfaces import AsyncFeatureStore, FeatureStore

STATUS_METHODS = ["is_monitoring_enabled", "is_available", "close"]


class CompleteStore(FeatureStore):
    def get(self, kind, key, callback=lambda x: x):
        return callback(None)

    def all(self, kind, callback=lambda x: x):
        return callback({})

    def init(self, all_data):
        pass

    def delete(self, kind, key, version):
        return False

    def upsert(self, kind, item):
        return False

    @property
    def initialized(self):
        return True

    def is_monitoring_enabled(self):
        return False

    def is_available(self):
        return True

    def close(self):
        pass


class CompleteAsyncStore(AsyncFeatureStore):
    async def get(self, kind, key):
        return None

    async def all(self, kind):
        return {}

    async def init(self, all_data):
        pass

    async def upsert(self, kind, item):
        return False

    async def delete(self, kind, key, version):
        return False

    @property
    def initialized(self):
        return True

    async def is_initialized(self):
        return True

    def is_monitoring_enabled(self):
        return False

    async def is_available(self):
        return True

    async def close(self):
        pass


def _without(base, interface, missing):
    # Copy every method of the complete store except the missing one. The copy
    # subclasses the interface directly, so the interface sees the gap.
    members = {name: value for name, value in vars(base).items() if not name.startswith("__") and name != missing}
    return type("IncompleteStore", (interface,), members)


def test_complete_store_can_be_instantiated():
    CompleteStore()
    CompleteAsyncStore()


@pytest.mark.parametrize("missing", STATUS_METHODS)
def test_store_without_status_method_cannot_be_instantiated(missing):
    store_class = _without(CompleteStore, FeatureStore, missing)
    with pytest.raises(TypeError):
        store_class()


@pytest.mark.parametrize("missing", STATUS_METHODS)
def test_async_store_without_status_method_cannot_be_instantiated(missing):
    store_class = _without(CompleteAsyncStore, AsyncFeatureStore, missing)
    with pytest.raises(TypeError):
        store_class()
