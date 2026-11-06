"""
Tests for the override marker carried on the flag and segment models. The marker lives on the
model type and never in the JSON representation.
"""
import json

import pytest

from ldclient.impl.model import FeatureFlag, ModelEncoder, Segment
from ldclient.testing.builders import FlagBuilder, SegmentBuilder
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

flag_data = {'key': 'flag1', 'version': 2, 'on': True, 'variations': [True, False], 'fallthrough': {'variation': 0}, 'salt': 'x'}
segment_data = {'key': 'seg1', 'version': 3, 'included': ['user1'], 'salt': 'y'}


@pytest.mark.parametrize("kind,data", [(FEATURES, flag_data), (SEGMENTS, segment_data)])
def test_marker_defaults_to_false(kind, data):
    entity = kind.decode(dict(data))
    assert entity.is_override is False


@pytest.mark.parametrize("kind,data", [(FEATURES, flag_data), (SEGMENTS, segment_data)])
def test_marked_copy_is_marked_and_source_is_not(kind, data):
    entity = kind.decode(dict(data))
    marked = entity.with_override_marker()
    assert marked.is_override is True
    assert entity.is_override is False
    assert type(marked) is type(entity)


@pytest.mark.parametrize("kind,data", [(FEATURES, flag_data), (SEGMENTS, segment_data)])
def test_marker_is_never_serialized(kind, data):
    entity = kind.decode(dict(data))
    marked = entity.with_override_marker()
    assert marked.to_json_dict() == data
    assert kind.encode(marked) == data
    assert json.loads(ModelEncoder().encode(marked)) == data
    assert 'is_override' not in marked.to_json_dict()
    assert 'override' not in json.dumps(marked.to_json_dict()).lower()


@pytest.mark.parametrize("kind,data", [(FEATURES, flag_data), (SEGMENTS, segment_data)])
def test_marked_copy_shares_the_definition(kind, data):
    entity = kind.decode(dict(data))
    marked = entity.with_override_marker()
    assert marked.key == entity.key
    assert marked.version == entity.version
    assert marked.to_json_dict() is entity.to_json_dict()
    assert marked == entity


def test_marked_flag_keeps_its_parsed_properties():
    flag = FlagBuilder('flag1').version(7).on(True).variations('a', 'b').fallthrough_variation(1).prerequisite('p', 0).track_events(True).build()
    marked = flag.with_override_marker()
    assert marked.on is True
    assert marked.variations == ['a', 'b']
    assert marked.fallthrough.variation == 1
    assert [p.key for p in marked.prerequisites] == ['p']
    assert marked.track_events is True
    assert marked.version == 7


def test_marked_segment_keeps_its_parsed_properties():
    segment = SegmentBuilder('seg1').version(4).included('a').excluded('b').build()
    marked = segment.with_override_marker()
    assert marked.included == {'a'}
    assert marked.excluded == {'b'}
    assert marked.version == 4


def test_marking_a_marked_copy_is_a_marked_copy():
    flag = FeatureFlag(dict(flag_data)).with_override_marker()
    again = flag.with_override_marker()
    assert again.is_override is True
    assert again is not flag
