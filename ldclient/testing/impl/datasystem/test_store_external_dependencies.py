"""
Tests for the store's use of a second dependency graph: a LaunchDarkly change to a flag or
segment that an override entry depends on notifies the override entry's flag as well.
"""
from typing import List, Optional

from ldclient.impl.datasystem.store import Store
from ldclient.impl.dependency_tracker import KindAndKey
from ldclient.impl.listeners import Listeners
from ldclient.impl.overrides import OverrideLayer
from ldclient.interfaces import (
    Change,
    ChangeSet,
    ChangeType,
    FlagChange,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.testing.builders import (
    FlagBuilder,
    FlagRuleBuilder,
    SegmentBuilder,
    make_clause_matching_segment_key
)
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


def _flag(key: str, version: int, prerequisite: Optional[str] = None, segment: Optional[str] = None) -> dict:
    builder = FlagBuilder(key).version(version).on(True).variations(True, False).fallthrough_variation(1)
    if prerequisite is not None:
        builder.prerequisite(prerequisite, 0)
    if segment is not None:
        builder.rules(FlagRuleBuilder().id('r').variation(0).clauses(make_clause_matching_segment_key(segment)).build())
    return builder.build().to_json_dict()


def _segment(key: str, version: int) -> dict:
    return SegmentBuilder(key).version(version).build().to_json_dict()


def _put(kind: ObjectKind, item: dict) -> Change:
    return Change(action=ChangeType.PUT, kind=kind, key=item['key'], version=item['version'], object=item)


def _full(*changes: Change) -> ChangeSet:
    return ChangeSet(intent_code=IntentCode.TRANSFER_FULL, changes=list(changes), selector=Selector.no_selector())


def _delta(*changes: Change) -> ChangeSet:
    return ChangeSet(intent_code=IntentCode.TRANSFER_CHANGES, changes=list(changes), selector=Selector.no_selector())


class Fixture:
    def __init__(self, layer: Optional[OverrideLayer] = None):
        self.notified: List[str] = []
        listeners = Listeners()
        listeners.add(lambda change: self.notified.append(change.key))
        self.store = Store(listeners, Listeners())
        if layer is not None:
            self.store.set_external_dependencies(layer)

    def take_notified(self) -> List[str]:
        result = sorted(self.notified)
        del self.notified[:]
        return result


def test_delta_to_a_prerequisite_notifies_the_override_flag_that_depends_on_it():
    layer = OverrideLayer()
    layer.set_all({'override-flag': _flag('override-flag', 1, prerequisite='ld-flag')}, {})
    f = Fixture(layer)
    f.store.apply(_full(_put(ObjectKind.FLAG, _flag('ld-flag', 1))), False)
    f.take_notified()

    f.store.apply(_delta(_put(ObjectKind.FLAG, _flag('ld-flag', 2))), False)
    assert f.take_notified() == ['ld-flag', 'override-flag']


def test_full_data_set_replacement_notifies_the_override_flags_that_depend_on_changed_items():
    layer = OverrideLayer()
    layer.set_all({'override-flag': _flag('override-flag', 1, segment='ld-segment')}, {})
    f = Fixture(layer)
    f.store.apply(_full(_put(ObjectKind.SEGMENT, _segment('ld-segment', 1)), _put(ObjectKind.FLAG, _flag('unrelated', 1))), False)
    f.take_notified()

    f.store.apply(_full(_put(ObjectKind.SEGMENT, _segment('ld-segment', 2)), _put(ObjectKind.FLAG, _flag('unrelated', 1))), False)
    # The segment is not a flag, so only the dependent override flag is notified.
    assert f.take_notified() == ['override-flag']


def test_dependency_chain_crosses_between_the_store_and_the_layer():
    # LaunchDarkly flag -> override flag -> LaunchDarkly segment: the chain enters the layer
    # and comes back out, and a segment change must reach the LaunchDarkly flag at the top.
    layer = OverrideLayer()
    layer.set_all({'override-flag': _flag('override-flag', 1, segment='ld-segment')}, {})
    f = Fixture(layer)
    f.store.apply(_full(
        _put(ObjectKind.SEGMENT, _segment('ld-segment', 1)),
        _put(ObjectKind.FLAG, _flag('ld-flag', 1, prerequisite='override-flag')),
    ), False)
    f.take_notified()

    f.store.apply(_delta(_put(ObjectKind.SEGMENT, _segment('ld-segment', 2))), False)
    assert f.take_notified() == ['ld-flag', 'override-flag']


def test_layer_replacement_changes_which_flags_a_store_change_notifies():
    layer = OverrideLayer()
    layer.set_all({'override-flag': _flag('override-flag', 1, prerequisite='ld-flag')}, {})
    f = Fixture(layer)
    f.store.apply(_full(_put(ObjectKind.FLAG, _flag('ld-flag', 1))), False)
    f.take_notified()

    layer.set_all({}, {})
    f.store.apply(_delta(_put(ObjectKind.FLAG, _flag('ld-flag', 2))), False)
    assert f.take_notified() == ['ld-flag']


def test_store_without_external_dependencies_notifies_only_its_own_dependents():
    f = Fixture()
    f.store.apply(_full(_put(ObjectKind.FLAG, _flag('ld-flag', 1)), _put(ObjectKind.FLAG, _flag('top', 1, prerequisite='ld-flag'))), False)
    f.take_notified()

    f.store.apply(_delta(_put(ObjectKind.FLAG, _flag('ld-flag', 2))), False)
    assert f.take_notified() == ['ld-flag', 'top']


def test_external_dependencies_do_not_hide_the_store_s_own_dependents():
    layer = OverrideLayer()
    layer.set_all({'override-flag': _flag('override-flag', 1, prerequisite='ld-flag')}, {})
    f = Fixture(layer)
    f.store.apply(_full(_put(ObjectKind.FLAG, _flag('ld-flag', 1)), _put(ObjectKind.FLAG, _flag('top', 1, prerequisite='ld-flag'))), False)
    f.take_notified()

    f.store.apply(_delta(_put(ObjectKind.FLAG, _flag('ld-flag', 2))), False)
    assert f.take_notified() == ['ld-flag', 'override-flag', 'top']
