"""
Sync/async public-API parity guard.

The SDK hand-maintains parallel sync (``foo.py``) and async (``async_foo.py``)
implementations. This test catches the most likely drift -- a public method or
property added, removed, or renamed on one side and forgotten on the other --
without flagging the legitimate body differences between siblings (async/await,
asyncio.gather vs ThreadPoolExecutor, reworded docstrings). Behavioral drift
inside a shared method body is the job of the contract suites, not a source diff.
"""
import inspect

import pytest

from ldclient.async_client import AsyncLDClient
from ldclient.async_feature_store import AsyncInMemoryFeatureStore
from ldclient.client import LDClient
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.async_evaluator import AsyncEvaluator
from ldclient.impl.evaluator import Evaluator
from ldclient.interfaces import (
    AsyncBigSegmentStoreStatusProvider,
    BigSegmentStoreStatusProvider
)
from ldclient.migrations import AsyncMigratorBuilder, MigratorBuilder


def _public_surface(cls) -> set:
    """All public names on the class -- methods AND properties (names not
    starting with ``_``, excluding the ``object`` baseline)."""
    return {n for n in dir(cls) if not n.startswith("_")} - set(dir(object))


# (sync_cls, async_cls, sync_only, async_only)
# The allowlists document intentionally one-sided public members, either
# sync-only or async-only. Keep them small and justified -- every entry is a
# place the two APIs deliberately differ.
PAIRS = [
    pytest.param(
        LDClient, AsyncLDClient,
        {"postfork"},
        {"start", "flush_and_wait"},
        id="client",
    ),
    pytest.param(
        InMemoryFeatureStore, AsyncInMemoryFeatureStore,
        set(),
        # is_initialized is an awaitable readiness check; the sync store exposes
        # readiness through the synchronous ``initialized`` property instead.
        {"close", "is_initialized"},
        id="feature_store",
    ),
    pytest.param(Evaluator, AsyncEvaluator, set(), set(), id="evaluator"),
    pytest.param(MigratorBuilder, AsyncMigratorBuilder, set(), set(), id="migrator_builder"),
    pytest.param(
        BigSegmentStoreStatusProvider, AsyncBigSegmentStoreStatusProvider,
        # status is a synchronous property on the sync provider; a property can't await,
        # so the async provider exposes the same readiness check as the get_status coroutine.
        {"status"},
        {"get_status"},
        id="big_segment_store_status_provider",
    ),
]


@pytest.mark.parametrize("sync_cls, async_cls, sync_only, async_only", PAIRS)
def test_public_surface_parity(sync_cls, async_cls, sync_only, async_only):
    sync_surface = _public_surface(sync_cls) - sync_only
    async_surface = _public_surface(async_cls) - async_only

    missing_on_async = sync_surface - async_surface
    missing_on_sync = async_surface - sync_surface

    assert not missing_on_async, (
        f"{async_cls.__name__} is missing public members present on "
        f"{sync_cls.__name__}: {sorted(missing_on_async)} -- add them to the async "
        f"sibling, or add to the allowlist if intentionally one-sided."
    )
    assert not missing_on_sync, (
        f"{sync_cls.__name__} is missing public members present on "
        f"{async_cls.__name__}: {sorted(missing_on_sync)} -- add them to the sync "
        f"sibling, or add to the allowlist if intentionally one-sided."
    )
