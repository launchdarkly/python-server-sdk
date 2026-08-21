# pylint: disable=missing-docstring

from typing import Any, Dict

import pytest

from ldclient.impl.datasystem.async_fdv1 import _AsyncReadOnlyFeatureStoreView
from ldclient.versioned_data_kind import FEATURES, VersionedDataKind


def _flag_dict(key: str, version: int) -> dict:
    return {
        "key": key,
        "version": version,
        "on": True,
        "variations": [True, False],
        "fallthrough": {"variation": 0},
    }


class FakeAsyncStore:
    """Async store shape: get/all are coroutines with no callback."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    async def get(self, kind: VersionedDataKind, key: str) -> Any:
        return self._data.get(key)

    async def all(self, kind: VersionedDataKind) -> Dict[str, Any]:
        return dict(self._data)


@pytest.mark.asyncio
async def test_async_feature_store_view_get_decodes_dict():
    raw = _flag_dict("flag-a", 1)
    view = _AsyncReadOnlyFeatureStoreView(FakeAsyncStore({"flag-a": raw}))

    result = await view.get(FEATURES, "flag-a")

    assert result == FEATURES.decode(raw)
    assert not isinstance(result, dict)


@pytest.mark.asyncio
async def test_async_feature_store_view_get_passes_through_model():
    decoded = FEATURES.decode(_flag_dict("flag-a", 1))
    view = _AsyncReadOnlyFeatureStoreView(FakeAsyncStore({"flag-a": decoded}))

    result = await view.get(FEATURES, "flag-a")

    assert result is decoded


@pytest.mark.asyncio
async def test_async_feature_store_view_get_missing_returns_none():
    view = _AsyncReadOnlyFeatureStoreView(FakeAsyncStore({}))
    assert await view.get(FEATURES, "missing") is None


@pytest.mark.asyncio
async def test_async_feature_store_view_all_decodes_each_value():
    raw_a = _flag_dict("flag-a", 1)
    decoded_b = FEATURES.decode(_flag_dict("flag-b", 1))
    view = _AsyncReadOnlyFeatureStoreView(FakeAsyncStore({"flag-a": raw_a, "flag-b": decoded_b}))

    result = await view.all(FEATURES)

    assert set(result.keys()) == {"flag-a", "flag-b"}
    assert result["flag-a"] == FEATURES.decode(raw_a)  # dict decoded
    assert result["flag-b"] is decoded_b               # model passed through
