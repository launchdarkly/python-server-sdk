import inspect
from typing import Iterable, List, Tuple, Union

import pytest
from mock import patch

from ldclient.impl.datasystem.async_store import AsyncStore
from ldclient.impl.datasystem.store import Store
from ldclient.impl.listeners import Listeners
from ldclient.impl.model.feature_flag import FeatureFlag
from ldclient.impl.model.segment import Segment
from ldclient.interfaces import (
    ChangeSet,
    ChangeSetBuilder,
    IntentCode,
    ObjectKind,
    Selector
)
from ldclient.testing.builders import (
    FlagBuilder,
    FlagRuleBuilder,
    SegmentBuilder,
    SegmentRuleBuilder,
    make_clause
)
from ldclient.testing.test_util import SpyListener
from ldclient.versioned_data_kind import FEATURES

Item = Union[FeatureFlag, Segment]


def _kind_of(item: Item) -> ObjectKind:
    return ObjectKind.FLAG if isinstance(item, FeatureFlag) else ObjectKind.SEGMENT


def full_transfer(items: Iterable[Item]) -> ChangeSet:
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_FULL)
    for item in items:
        builder.add_put(_kind_of(item), item.key, item.version, item.to_json_dict())
    return builder.finish(Selector.new_selector('full', 1))


def delta(puts: Iterable[Item] = (), deletes: Iterable[Tuple[ObjectKind, str, int]] = ()) -> ChangeSet:
    builder = ChangeSetBuilder()
    builder.start(IntentCode.TRANSFER_CHANGES)
    for item in puts:
        builder.add_put(_kind_of(item), item.key, item.version, item.to_json_dict())
    for kind, key, version in deletes:
        builder.add_delete(kind, key, version)
    return builder.finish(Selector.new_selector('delta', 2))


def segment_match_rule(segment_key: str) -> dict:
    return FlagRuleBuilder().variation(0).id('rule_id').clauses(make_clause('user', 'segmentMatch', 'segmentMatch', segment_key)).build()


def basic_data() -> List[Item]:
    return [
        FlagBuilder('flag1').version(1).on(False).build(),
        FlagBuilder('flag2').version(1).on(False).build(),
        FlagBuilder('flag3').version(1).rules(segment_match_rule('segment2')).build(),
        SegmentBuilder('segment1').version(1).build(),
        SegmentBuilder('segment2').version(1).build(),
    ]


def prereq_data() -> List[Item]:
    # flag1 -> flag2 -> (flag3, flag4, flag6); flag6 -> segment2 -> segment1.
    return [
        FlagBuilder('flag1').version(1).on(False).prerequisite('flag2', 0).build(),
        FlagBuilder('flag2').version(1).on(False).prerequisite('flag3', 0).prerequisite('flag4', 0).prerequisite('flag6', 0).build(),
        FlagBuilder('flag3').version(1).on(False).build(),
        FlagBuilder('flag4').version(1).on(False).build(),
        FlagBuilder('flag5').version(1).on(False).build(),
        FlagBuilder('flag6').version(1).rules(segment_match_rule('segment2')).build(),
        SegmentBuilder('segment1').version(1).build(),
        SegmentBuilder('segment2').version(1).rules(SegmentRuleBuilder().clauses(make_clause('user', 'segmentMatch', 'segmentMatch', 'segment1')).build()).build(),
    ]


class Harness:
    """Wraps a sync or async FDv2 store so tests drive both the same way."""

    def __init__(self, kind: str):
        self.flag_change_listeners = Listeners()
        store_class = Store if kind == 'sync' else AsyncStore
        self.store = store_class(self.flag_change_listeners, Listeners())

    async def apply(self, change_set: ChangeSet) -> None:
        result = self.store.apply(change_set, False)
        if inspect.isawaitable(result):
            await result

    def listen(self) -> SpyListener:
        spy = SpyListener()
        self.flag_change_listeners.add(spy)
        return spy


@pytest.fixture(params=['sync', 'async'])
def harness(request) -> Harness:
    return Harness(request.param)


def changed_keys(spy: SpyListener) -> List[str]:
    # Events arrive in no guaranteed order, so sort them.
    return sorted(change.key for change in spy.statuses)


@pytest.mark.asyncio
async def test_first_full_transfer_notifies_for_every_flag(harness):
    spy = harness.listen()

    await harness.apply(full_transfer(basic_data()))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag3']


@pytest.mark.asyncio
async def test_full_transfer_notifies_only_for_changed_added_and_removed_flags(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    # flag1 changes version, flag4 is new, and flag2 and flag3 are gone.
    await harness.apply(full_transfer([
        FlagBuilder('flag1').version(2).on(False).build(),
        FlagBuilder('flag4').version(1).on(False).build(),
    ]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag3', 'flag4']


@pytest.mark.asyncio
async def test_full_transfer_with_identical_data_does_not_notify(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(full_transfer(prereq_data()))

    assert spy.statuses == []


@pytest.mark.asyncio
async def test_full_transfer_notifies_flags_that_depend_on_a_changed_segment(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    data = [item for item in prereq_data() if item.key != 'segment1']
    data.append(SegmentBuilder('segment1').version(2).build())
    await harness.apply(full_transfer(data))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag6']


@pytest.mark.asyncio
async def test_put_flag_notifies_for_that_flag(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[FlagBuilder('flag1').version(2).on(False).build()]))

    assert changed_keys(spy) == ['flag1']


@pytest.mark.asyncio
async def test_put_segment_notifies_flags_that_reference_it(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[SegmentBuilder('segment2').version(2).build()]))

    assert changed_keys(spy) == ['flag3']


@pytest.mark.asyncio
async def test_put_unreferenced_segment_does_not_notify(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[SegmentBuilder('segment1').version(2).build()]))

    assert spy.statuses == []


@pytest.mark.asyncio
async def test_put_prerequisite_notifies_dependent_flags_transitively(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[FlagBuilder('flag4').version(2).on(False).build()]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag4']


@pytest.mark.asyncio
async def test_adding_a_prerequisite_notifies_the_flag_and_its_dependents(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[FlagBuilder('flag3').version(2).on(False).prerequisite('flag4', 0).build()]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag3']


@pytest.mark.asyncio
async def test_removing_a_prerequisite_notifies_the_flag_and_its_dependents(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[FlagBuilder('flag2').version(2).on(False).prerequisite('flag3', 0).build()]))

    assert changed_keys(spy) == ['flag1', 'flag2']


@pytest.mark.asyncio
async def test_removed_prerequisite_no_longer_propagates_changes(harness):
    await harness.apply(full_transfer(prereq_data()))
    await harness.apply(delta(puts=[FlagBuilder('flag2').version(2).on(False).prerequisite('flag3', 0).build()]))
    spy = harness.listen()

    await harness.apply(delta(puts=[FlagBuilder('flag4').version(2).on(False).build()]))

    assert changed_keys(spy) == ['flag4']


@pytest.mark.asyncio
async def test_delete_flag_notifies_for_that_flag(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    await harness.apply(delta(deletes=[(ObjectKind.FLAG, 'flag1', 2)]))

    assert changed_keys(spy) == ['flag1']


@pytest.mark.asyncio
async def test_delete_prerequisite_notifies_dependent_flags_transitively(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(deletes=[(ObjectKind.FLAG, 'flag4', 2)]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag4']


@pytest.mark.asyncio
async def test_put_segment_notifies_through_segment_rules_and_prerequisites(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(puts=[SegmentBuilder('segment1').version(2).build()]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag6']


@pytest.mark.asyncio
async def test_delete_segment_notifies_dependent_flags_transitively(harness):
    await harness.apply(full_transfer(prereq_data()))
    spy = harness.listen()

    await harness.apply(delta(deletes=[(ObjectKind.SEGMENT, 'segment2', 2)]))

    assert changed_keys(spy) == ['flag1', 'flag2', 'flag6']


@pytest.mark.asyncio
async def test_transfer_none_does_not_notify(harness):
    await harness.apply(full_transfer(basic_data()))
    spy = harness.listen()

    await harness.apply(ChangeSetBuilder.no_changes())

    assert spy.statuses == []


@pytest.mark.asyncio
async def test_no_listener_skips_change_computation(harness):
    store = harness.store
    with patch.object(store._dependency_tracker, 'add_affected_items') as add_affected, \
            patch.object(store, '_compute_changed_items_for_full_data_set') as compute_full, \
            patch.object(store._memory_store, 'all', wraps=store._memory_store.all) as read_all:
        await harness.apply(full_transfer(prereq_data()))
        await harness.apply(full_transfer(basic_data()))
        await harness.apply(delta(puts=[FlagBuilder('flag1').version(2).build()]))

    add_affected.assert_not_called()
    compute_full.assert_not_called()
    read_all.assert_not_called()

    # The store still applies the data.
    assert store.get_active_store().get(FEATURES, 'flag1').version == 2
