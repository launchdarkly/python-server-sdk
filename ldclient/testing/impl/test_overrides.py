"""
Tests for the override layer, the overlay at the store read boundary, and the sink that applies
snapshots and notifies flag change listeners.
"""
from typing import Any, Dict, List, Optional

import pytest

from ldclient.impl.datasystem.store import InMemoryFeatureStore
from ldclient.impl.overrides import (
    AsyncOverrideStoreView,
    OverrideLayer,
    OverrideSinkImpl,
    OverrideStoreView
)
from ldclient.testing.builders import (
    FlagBuilder,
    FlagRuleBuilder,
    SegmentBuilder,
    make_clause_matching_segment_key
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class FakeBaseStore:
    """A minimal read-only store with controllable contents, initialization state, and failures."""

    def __init__(self, flags: Optional[Dict[str, Any]] = None, segments: Optional[Dict[str, Any]] = None, initialized: bool = True):
        self.flags = dict(flags or {})
        self.segments = dict(segments or {})
        self._initialized = initialized
        self.fail_all = False

    def _items(self, kind):
        return self.flags if kind == FEATURES else self.segments

    def get(self, kind, key, callback=lambda x: x):
        return callback(self._items(kind).get(key))

    def all(self, kind, callback=lambda x: x):
        if self.fail_all:
            raise RuntimeError("store failure")
        return callback(dict(self._items(kind)))

    @property
    def initialized(self) -> bool:
        return self._initialized


class FakeAsyncBaseStore:
    def __init__(self, base: FakeBaseStore):
        self._base = base

    async def get(self, kind, key):
        return self._base.get(kind, key)

    async def all(self, kind):
        return self._base.all(kind)


def flag(key: str, version: int = 1):
    return FlagBuilder(key).version(version).on(True).variations(False, True).fallthrough_variation(1).off_variation(0).build()


def segment(key: str, version: int = 1):
    return SegmentBuilder(key).version(version).build()


# ---------------------------------------------------------------------------
# Layer
# ---------------------------------------------------------------------------

def test_layer_marks_copies_without_mutating_source():
    layer = OverrideLayer()
    f = flag('flag1', 2)
    s = segment('segment1', 3)
    layer.set_all({'flag1': f}, {'segment1': s})

    assert f.is_override is False
    assert s.is_override is False
    stored_flag = layer.get(FEATURES, 'flag1')
    assert stored_flag is not None
    assert stored_flag.is_override is True
    assert stored_flag.version == 2
    stored_segment = layer.get(SEGMENTS, 'segment1')
    assert stored_segment is not None
    assert stored_segment.is_override is True


def test_layer_decodes_dictionaries_and_marks_them():
    layer = OverrideLayer()
    layer.set_all({'flag1': flag('flag1').to_json_dict()}, {'segment1': segment('segment1').to_json_dict()})
    stored_flag = layer.get(FEATURES, 'flag1')
    assert stored_flag is not None
    assert stored_flag.is_override is True
    assert stored_flag.on is True
    stored_segment = layer.get(SEGMENTS, 'segment1')
    assert stored_segment is not None
    assert stored_segment.is_override is True


def test_layer_rejects_invalid_definitions_and_keeps_its_contents():
    layer = OverrideLayer()
    layer.set_all({'flag1': flag('flag1')}, {})
    with pytest.raises(ValueError):
        layer.set_all({'flag2': {'key': 'flag2', 'version': 'not a number'}}, {})
    assert layer.get(FEATURES, 'flag1') is not None
    assert layer.get(FEATURES, 'flag2') is None
    with pytest.raises(ValueError):
        layer.set_all({}, {'seg': {'key': 'seg', 'version': 1, 'included': 'not a list'}})
    assert layer.get(FEATURES, 'flag1') is not None


def test_layer_replacement_semantics():
    layer = OverrideLayer()
    assert layer.is_empty is True
    assert layer.get(FEATURES, 'flag1') is None

    layer.set_all({'flag1': flag('flag1')}, {})
    assert layer.is_empty is False
    assert layer.get(FEATURES, 'flag1') is not None

    # A replacement is a full snapshot: entries absent from it are removed.
    layer.set_all({'flag2': flag('flag2')}, {})
    assert layer.get(FEATURES, 'flag1') is None
    assert layer.get(FEATURES, 'flag2') is not None

    layer.set_all({}, {})
    assert layer.is_empty is True
    assert layer.get(FEATURES, 'flag2') is None


def test_layer_set_all_returns_previous_and_current_contents():
    layer = OverrideLayer()
    previous, current = layer.set_all({'flag1': flag('flag1')}, {'seg': segment('seg')})
    assert previous == {FEATURES: {}, SEGMENTS: {}}
    assert list(current[FEATURES].keys()) == ['flag1']
    assert list(current[SEGMENTS].keys()) == ['seg']
    previous, current = layer.set_all({}, {})
    assert list(previous[FEATURES].keys()) == ['flag1']
    assert current == {FEATURES: {}, SEGMENTS: {}}


def test_layer_all_returns_entries_of_a_kind():
    layer = OverrideLayer()
    layer.set_all({'a': flag('a'), 'b': flag('b')}, {'s': segment('s')})
    assert sorted(layer.all(FEATURES).keys()) == ['a', 'b']
    assert list(layer.all(SEGMENTS).keys()) == ['s']
    assert all(item.is_override for item in layer.all(FEATURES).values())


# ---------------------------------------------------------------------------
# Overlay
# ---------------------------------------------------------------------------

def test_overlay_get_precedence():
    base = FakeBaseStore(flags={'both': flag('both', 1), 'base-only': flag('base-only', 1)})
    layer = OverrideLayer()
    layer.set_all({'both': flag('both', 99), 'override-only': flag('override-only', 1)}, {})
    overlay = OverrideStoreView(base, layer)

    item = overlay.get(FEATURES, 'both')
    assert item.version == 99
    assert item.is_override is True

    item = overlay.get(FEATURES, 'base-only')
    assert item.is_override is False

    item = overlay.get(FEATURES, 'override-only')
    assert item.is_override is True

    assert overlay.get(FEATURES, 'nowhere') is None
    assert overlay.get(FEATURES, 'both', lambda x: x.version) == 99


def test_overlay_get_serves_overrides_from_uninitialized_base():
    base = FakeBaseStore(initialized=False)
    layer = OverrideLayer()
    layer.set_all({'flag1': flag('flag1')}, {})
    overlay = OverrideStoreView(base, layer)
    item = overlay.get(FEATURES, 'flag1')
    assert item.is_override is True
    assert overlay.initialized is False


def test_overlay_get_all_union():
    memory = InMemoryFeatureStore()
    memory.set_basis({
        FEATURES: {
            'both': flag('both', 1).to_json_dict(),
            'base-only': flag('base-only', 1).to_json_dict(),
            'tombstone': {'key': 'tombstone', 'version': 5, 'deleted': True},
        },
        SEGMENTS: {},
    })
    layer = OverrideLayer()
    layer.set_all({'both': flag('both', 99), 'tombstone': flag('tombstone', 1), 'override-only': flag('override-only', 1)}, {})
    overlay = OverrideStoreView(memory, layer)

    items = overlay.all(FEATURES)
    assert sorted(items.keys()) == ['base-only', 'both', 'override-only', 'tombstone']
    assert items['both'].version == 99
    assert items['both'].is_override is True
    assert items['base-only'].is_override is False
    assert items['tombstone'].is_override is True, "an override wins over a deleted item"
    assert items['override-only'].is_override is True
    assert overlay.all(FEATURES, lambda x: len(x)) == 4


def test_overlay_get_all_with_empty_layer_is_passthrough():
    base = FakeBaseStore(flags={'flag1': flag('flag1')})
    overlay = OverrideStoreView(base, OverrideLayer())
    assert list(overlay.all(FEATURES).keys()) == ['flag1']
    base.fail_all = True
    with pytest.raises(RuntimeError):
        overlay.all(FEATURES)


def test_overlay_get_all_serves_overrides_when_base_fails():
    base = FakeBaseStore()
    base.fail_all = True
    layer = OverrideLayer()
    layer.set_all({'override-1': flag('override-1', 1), 'override-2': flag('override-2', 2)}, {})
    overlay = OverrideStoreView(base, layer)
    items = overlay.all(FEATURES)
    assert sorted(items.keys()) == ['override-1', 'override-2']
    assert all(item.is_override for item in items.values())


def test_overlay_initialized_follows_the_base():
    base = FakeBaseStore(initialized=True)
    overlay = OverrideStoreView(base, OverrideLayer())
    assert overlay.initialized is True
    base._initialized = False
    assert overlay.initialized is False


@pytest.mark.asyncio
async def test_async_overlay_get_and_all():
    base = FakeBaseStore(flags={'both': flag('both', 1), 'base-only': flag('base-only', 1)}, initialized=False)
    layer = OverrideLayer()
    layer.set_all({'both': flag('both', 99), 'override-only': flag('override-only', 1)}, {'seg': segment('seg')})
    overlay = AsyncOverrideStoreView(FakeAsyncBaseStore(base), layer)

    both = await overlay.get(FEATURES, 'both')
    assert both.version == 99 and both.is_override is True
    base_only = await overlay.get(FEATURES, 'base-only')
    assert base_only.is_override is False
    assert (await overlay.get(FEATURES, 'nowhere')) is None
    assert (await overlay.get(SEGMENTS, 'seg')).is_override is True

    items = await overlay.all(FEATURES)
    assert sorted(items.keys()) == ['base-only', 'both', 'override-only']
    assert items['both'].version == 99


@pytest.mark.asyncio
async def test_async_overlay_all_when_base_fails():
    base = FakeBaseStore()
    base.fail_all = True
    layer = OverrideLayer()
    overlay = AsyncOverrideStoreView(FakeAsyncBaseStore(base), layer)
    with pytest.raises(RuntimeError):
        await overlay.all(FEATURES)
    layer.set_all({'override-1': flag('override-1')}, {})
    items = await overlay.all(FEATURES)
    assert list(items.keys()) == ['override-1']


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

class SinkFixture:
    def __init__(self, base: FakeBaseStore):
        self.base = base
        self.layer = OverrideLayer()
        self.notified: List[str] = []
        self.listening = True
        self.sink = OverrideSinkImpl(self.layer, base, lambda key: self.notified.append(key), lambda: self.listening)

    def take_notified(self) -> List[str]:
        result = sorted(self.notified)
        del self.notified[:]
        return result


def test_sink_notifies_on_add_change_remove():
    base = FakeBaseStore(flags={'flag1': flag('flag1', 1)})
    f = SinkFixture(base)

    # Adding an override is a change even though flag1 also exists in base data.
    f.sink.set_overrides({'flag1': flag('flag1', 1), 'flag2': flag('flag2', 1)}, {})
    assert f.take_notified() == ['flag1', 'flag2']

    # An identical replacement (rebuilt from scratch, new objects) changes nothing.
    f.sink.set_overrides({'flag1': flag('flag1', 1), 'flag2': flag('flag2', 1)}, {})
    assert f.take_notified() == []

    # Changing one entry notifies only that entry.
    f.sink.set_overrides({'flag1': flag('flag1', 1), 'flag2': flag('flag2', 2)}, {})
    assert f.take_notified() == ['flag2']

    # A change in content at the same version is a change.
    f.sink.set_overrides({'flag1': flag('flag1', 1), 'flag2': FlagBuilder('flag2').version(2).on(False).build()}, {})
    assert f.take_notified() == ['flag2']

    # Removing overrides notifies them: flag1 reverts to base data, flag2 to not-found.
    f.sink.set_overrides({}, {})
    assert f.take_notified() == ['flag1', 'flag2']


def test_sink_segment_override_fans_out_to_dependent_flags():
    dependent = FlagBuilder('dependent').version(1).on(True).variations(True, False).fallthrough_variation(1).rules(
        FlagRuleBuilder().id('r').variation(0).clauses(make_clause_matching_segment_key('segment1')).build()
    ).build()
    base = FakeBaseStore(
        flags={'dependent': dependent, 'unrelated': flag('unrelated')},
        segments={'segment1': segment('segment1', 1)},
    )
    f = SinkFixture(base)
    f.sink.set_overrides({}, {'segment1': segment('segment1', 99)})
    # The segment itself is not a flag, so only the dependent flag is notified.
    assert f.take_notified() == ['dependent']


def test_sink_prerequisite_override_fans_out_through_the_chain():
    top = FlagBuilder('top').version(1).on(True).variations(True, False).fallthrough_variation(0).prerequisite('mid', 0).build()
    mid = FlagBuilder('mid').version(1).on(True).variations(True, False).fallthrough_variation(0).prerequisite('leaf', 0).build()
    base = FakeBaseStore(flags={'top': top, 'mid': mid, 'leaf': flag('leaf'), 'other': flag('other')})
    f = SinkFixture(base)
    f.sink.set_overrides({'leaf': flag('leaf', 5)}, {})
    assert f.take_notified() == ['leaf', 'mid', 'top']


def test_sink_removing_a_segment_override_notifies_flags_that_reference_it():
    # The override of "dependent" references segment "s". Removing the override of "s" alone
    # changes what "dependent" evaluates to, so "dependent" is notified through the dependency
    # fan-out even though its own override did not change.
    dependent_override = FlagBuilder('dependent').version(2).on(True).variations(True, False).fallthrough_variation(1).rules(
        FlagRuleBuilder().variation(0).clauses(make_clause_matching_segment_key('s')).build()
    ).build()
    base = FakeBaseStore(flags={'dependent': flag('dependent')}, segments={'s': segment('s')})
    f = SinkFixture(base)
    f.sink.set_overrides({'dependent': dependent_override}, {'s': segment('s', 2)})
    f.take_notified()
    f.sink.set_overrides({'dependent': dependent_override}, {})
    assert f.take_notified() == ['dependent']


def test_sink_skips_change_computation_when_nothing_listens():
    base = FakeBaseStore(flags={'flag1': flag('flag1')})
    f = SinkFixture(base)
    f.listening = False
    f.sink.set_overrides({'flag1': flag('flag1', 2)}, {})
    assert f.take_notified() == []
    assert f.layer.get(FEATURES, 'flag1').version == 2


def test_sink_notifies_directly_changed_keys_when_base_read_fails():
    base = FakeBaseStore(flags={'flag1': flag('flag1')})
    base.fail_all = True
    f = SinkFixture(base)
    f.sink.set_overrides({'flag1': flag('flag1', 2)}, {})
    assert f.take_notified() == ['flag1']


def test_sink_invalid_snapshot_raises_and_changes_nothing():
    base = FakeBaseStore()
    f = SinkFixture(base)
    f.sink.set_overrides({'flag1': flag('flag1')}, {})
    f.take_notified()
    with pytest.raises(ValueError):
        f.sink.set_overrides({'flag1': {'key': 'flag1', 'version': 'bad'}}, {})
    assert f.layer.get(FEATURES, 'flag1').version == 1
    assert f.take_notified() == []
