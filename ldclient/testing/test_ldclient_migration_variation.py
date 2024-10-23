import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.migrations import Operation, Origin, Stage
from ldclient.testing.builders import FlagBuilder
from ldclient.testing.test_ldclient import make_client, user
from ldclient.versioned_data_kind import FEATURES


def test_uses_default_if_flag_not_found():
    store = InMemoryFeatureStore()
    client = make_client(store)

    stage, tracker = client.migration_variation('key', user, Stage.LIVE)

    assert stage == Stage.LIVE
    assert tracker is not None


def test_off_if_default_is_bad():
    store = InMemoryFeatureStore()
    client = make_client(store)

    stage, tracker = client.migration_variation('key', user, 'invalid default stage')

    assert stage == Stage.OFF
    assert tracker is not None


def test_uses_default_if_flag_returns_invalid_stage():
    feature = FlagBuilder('key').on(True).variations('i am not', 'a valid', 'migration flag').fallthrough_variation(1).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key': feature}})
    client = make_client(store)

    stage, tracker = client.migration_variation('key', user, Stage.LIVE)
    tracker.operation(Operation.READ)
    tracker.invoked(Origin.OLD)

    assert stage == Stage.LIVE
    assert tracker is not None

    event = tracker.build()
    assert event.detail.value == Stage.LIVE.value
    assert event.detail.variation_index is None
    assert event.detail.reason["errorKind"] == "WRONG_TYPE"


@pytest.mark.parametrize(
    "expected,default",
    [
        pytest.param(Stage.OFF, Stage.DUALWRITE, id="off"),
        pytest.param(Stage.DUALWRITE, Stage.SHADOW, id="dualwrite"),
        pytest.param(Stage.SHADOW, Stage.LIVE, id="shadow"),
        pytest.param(Stage.LIVE, Stage.RAMPDOWN, id="live"),
        pytest.param(Stage.RAMPDOWN, Stage.COMPLETE, id="rampdown"),
        pytest.param(Stage.COMPLETE, Stage.OFF, id="complete"),
    ],
)
def test_can_determine_correct_stage(expected: Stage, default: Stage):
    feature = FlagBuilder('key').on(True).variations(expected.value).fallthrough_variation(0).build()
    store = InMemoryFeatureStore()
    store.init({FEATURES: {'key': feature}})
    client = make_client(store)

    stage, tracker = client.migration_variation('key', user, default)

    assert stage == expected
    assert tracker is not None
