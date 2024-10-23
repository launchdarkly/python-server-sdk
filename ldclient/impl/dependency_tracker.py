from typing import Dict, List, NamedTuple, Optional, Set, Union

from ldclient.impl.model.clause import Clause
from ldclient.impl.model.feature_flag import FeatureFlag
from ldclient.impl.model.segment import Segment
from ldclient.versioned_data_kind import FEATURES, SEGMENTS, VersionedDataKind


class KindAndKey(NamedTuple):
    kind: VersionedDataKind
    key: str


class DependencyTracker:
    """
    The DependencyTracker is responsible for tracking both up and downstream
    dependency relationships. Managing a bi-directional mapping allows us to
    more easily perform updates to the tracker, and to determine affected items
    when a downstream item is modified.
    """

    def __init__(self):
        self.__children: Dict[KindAndKey, Set[KindAndKey]] = {}
        self.__parents: Dict[KindAndKey, Set[KindAndKey]] = {}

    def update_dependencies_from(self, from_kind: VersionedDataKind, from_key: str, from_item: Optional[Union[dict, FeatureFlag, Segment]]):
        """
        Updates the dependency graph when an item has changed.

        :param from_kind: the changed item's kind
        :param from_key: the changed item's key
        :param from_item: the changed item

        """
        from_what = KindAndKey(kind=from_kind, key=from_key)
        updated_dependencies = DependencyTracker.compute_dependencies_from(from_kind, from_item)

        old_children_set = self.__children.get(from_what)

        if old_children_set is not None:
            for kind_and_key in old_children_set:
                parents_of_this_old_dep = self.__parents.get(kind_and_key, set())
                if from_what in parents_of_this_old_dep:
                    parents_of_this_old_dep.remove(from_what)

        self.__children[from_what] = updated_dependencies
        for kind_and_key in updated_dependencies:
            parents_of_this_new_dep = self.__parents.get(kind_and_key)
            if parents_of_this_new_dep is None:
                parents_of_this_new_dep = set()
                self.__parents[kind_and_key] = parents_of_this_new_dep

            parents_of_this_new_dep.add(from_what)

    def add_affected_items(self, items_out: Set[KindAndKey], initial_modified_item: KindAndKey):
        """

        Populates the given set with the union of the initial item and all items that directly or indirectly
        depend on it (based on the current state of the dependency graph).

        @param items_out [Set]
        @param initial_modified_item [Object]

        """

        if initial_modified_item in items_out:
            return

        items_out.add(initial_modified_item)

        parents = self.__parents.get(initial_modified_item)
        if parents is None:
            return

        for parent in parents:
            self.add_affected_items(items_out, parent)

    def reset(self):
        """
        Clear any tracked dependencies and reset the tracking state to a clean slate.
        """
        self.__children.clear()
        self.__parents.clear()

    @staticmethod
    def compute_dependencies_from(from_kind: VersionedDataKind, from_item: Optional[Union[dict, FeatureFlag, Segment]]) -> Set[KindAndKey]:
        """
        @param from_kind [String]
        @param from_item [LaunchDarkly::Impl::Model::FeatureFlag, LaunchDarkly::Impl::Model::Segment]
        @return [Set]
        """
        if from_item is None:
            return set()

        from_item = from_kind.decode(from_item) if isinstance(from_item, dict) else from_item

        if from_kind == FEATURES and isinstance(from_item, FeatureFlag):
            prereq_keys = [KindAndKey(kind=from_kind, key=p.key) for p in from_item.prerequisites]
            segment_keys = [kindAndKey for rule in from_item.rules for kindAndKey in DependencyTracker.segment_keys_from_clauses(rule.clauses)]

            results = set(prereq_keys)
            results.update(segment_keys)

            return results
        elif from_kind == SEGMENTS and isinstance(from_item, Segment):
            kind_and_keys = [key for rule in from_item.rules for key in DependencyTracker.segment_keys_from_clauses(rule.clauses)]
            return set(kind_and_keys)
        else:
            return set()

    @staticmethod
    def segment_keys_from_clauses(clauses: List[Clause]) -> List[KindAndKey]:
        results = []
        for clause in clauses:
            if clause.op == 'segmentMatch':
                pairs = [KindAndKey(kind=SEGMENTS, key=value) for value in clause.values]
                results.extend(pairs)

        return results
