import re

import pytest
from semver import VersionInfo

from ldclient.impl.model import *
from ldclient.testing.builders import *
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


def test_flag_targets_are_stored_as_sets():
    flag = FlagBuilder("key").target(0, "a", "b").context_target("kind1", 0, "c", "d").build()
    assert flag.targets[0].values == {"a", "b"}
    assert flag.context_targets[0].values == {"c", "d"}


def test_segment_targets_are_stored_as_sets():
    segment = SegmentBuilder("key").included("a", "b").excluded("c", "d").included_contexts("kind1", "e", "f").excluded_contexts("kind2", "g", "h").build()
    assert segment.included == {"a", "b"}
    assert segment.excluded == {"c", "d"}
    assert segment.included_contexts[0].values == {"e", "f"}
    assert segment.excluded_contexts[0].values == {"g", "h"}


def test_clause_values_preprocessed_with_regex_operator():
    pattern_str = "^[a-z]*$"
    pattern = re.compile(pattern_str)
    flag = make_boolean_flag_with_clauses(make_clause(None, "attr", "matches", pattern_str, "?", True))
    assert flag.rules[0].clauses[0]._values == [pattern_str, "?", True]
    assert list(x.as_regex for x in flag.rules[0].clauses[0]._values_preprocessed) == [pattern, None, None]


@pytest.mark.parametrize('op', ['semVerEqual', 'semVerGreaterThan', 'semVerLessThan'])
def test_clause_values_preprocessed_with_semver_operator(op):
    flag = make_boolean_flag_with_clauses(make_clause(None, "attr", op, "1.2.3", 1, True))
    assert flag.rules[0].clauses[0]._values == ["1.2.3", 1, True]
    assert list(x.as_semver for x in flag.rules[0].clauses[0]._values_preprocessed) == [VersionInfo(1, 2, 3), None, None]


@pytest.mark.parametrize('op', ['before', 'after'])
def test_clause_values_preprocessed_with_time_operator(op):
    flag = make_boolean_flag_with_clauses(make_clause(None, "attr", op, 1000, "1970-01-01T00:00:02Z", True))
    assert flag.rules[0].clauses[0]._values == [1000, "1970-01-01T00:00:02Z", True]
    assert list(x.as_time for x in flag.rules[0].clauses[0]._values_preprocessed) == [1000, 2000, None]


@pytest.mark.parametrize('kind', [FEATURES, SEGMENTS])
def test_tombstone_without_key_can_be_decoded(kind):
    # Other LaunchDarkly SDKs write deleted items to a persistent store with only the version,
    # so we must be able to read them back.
    item = kind.decode({"version": 5, "deleted": True})
    assert item.version == 5
    assert item.deleted is True
    assert item.key == ''
    # The original data must round-trip unchanged, because the store re-serializes it.
    assert item.to_json_dict() == {"version": 5, "deleted": True}


@pytest.mark.parametrize('kind', [FEATURES, SEGMENTS])
def test_tombstone_with_placeholder_key_can_be_decoded(kind):
    # The Go SDK and the Relay Proxy write deleted items with a placeholder key.
    item = kind.decode({"key": "$deleted", "version": 5, "deleted": True})
    assert item.version == 5
    assert item.deleted is True
    assert item.key == '$deleted'


@pytest.mark.parametrize('kind', [FEATURES, SEGMENTS])
def test_tombstone_still_requires_version(kind):
    with pytest.raises(ValueError):
        kind.decode({"deleted": True})


@pytest.mark.parametrize('kind', [FEATURES, SEGMENTS])
def test_item_that_is_not_deleted_still_requires_key(kind):
    with pytest.raises(ValueError):
        kind.decode({"version": 5})
    with pytest.raises(ValueError):
        kind.decode({"version": 5, "deleted": False})
