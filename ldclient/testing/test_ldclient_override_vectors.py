"""
Runs the OVERRIDE specification test vectors. Each vector sets up LaunchDarkly data, an
override layer, and an initialization state. The test evaluates one flag through the full
client stack and checks the value, the variation index, the reason, and the marking handed to the
event processor.
"""
import json
import os
from typing import Any, Dict

import pytest

from ldclient.client import Config, Context, LDClient
from ldclient.datasystem import custom
from ldclient.impl.events.types import EventInputEvaluation
from ldclient.impl.integrations.files.filedata import make_flag_with_value
from ldclient.testing.mock_components import (
    HangingSynchronizer,
    MockOverrideSource,
    StaticInitializer
)
from ldclient.testing.stub_util import MockEventProcessor

VECTORS_PATH = os.path.join(os.path.dirname(__file__), 'testdata', 'override-vectors', 'vectors.json')

# The vectors' semantics are versioned. A schema change means this runner needs review.
SUPPORTED_SCHEMA_VERSION = '0.4.0'


def load_vectors():
    with open(VECTORS_PATH, 'r') as f:
        document = json.load(f)
    assert document['schemaVersion'] == SUPPORTED_SCHEMA_VERSION, "the vectors changed schema; review this runner against the new schema before updating"
    assert len(document['vectors']) > 0
    return document['vectors']


def vector_id(vector: Dict[str, Any]) -> str:
    return "%s: %s" % (vector['group'], vector['description'])


def override_flags(overrides: Dict[str, Any]) -> Dict[str, Any]:
    flags = dict(overrides.get('flags', {}))
    for key, value in overrides.get('flagValues', {}).items():
        flags[key] = make_flag_with_value(key, value)
    return flags


def make_client(vector: Dict[str, Any]) -> LDClient:
    source = MockOverrideSource(flags=override_flags(vector['overrides']), segments=dict(vector['overrides'].get('segments', {})))
    ld_data = vector['launchDarklyData']
    datasystem = custom().overrides(source.builder)
    if ld_data['initialized']:
        datasystem.initializers([StaticInitializer(ld_data.get('flags', {}), ld_data.get('segments', {})).builder])
        start_wait = 5
    else:
        # With no sources at all, the client would consider cached data available rather than
        # applying its not-initialized handling. A synchronizer that never delivers avoids that.
        datasystem.synchronizers(HangingSynchronizer().builder)
        start_wait = 0
    config = Config(sdk_key='SDK_KEY', datasystem_config=datasystem.build(), event_processor_class=MockEventProcessor)
    return LDClient(config, start_wait=start_wait)


def assert_reason(expected: Dict[str, Any], actual: Dict[str, Any]) -> None:
    """
    The reason is compared only on the fields present in the expected reason. The
    override-affected indicator is a tri-state collapse: an expected reason that omits it
    requires the actual reason to omit it or report it as false.
    """
    for name, value in expected.items():
        assert actual.get(name) == value, "reason property %s: expected %r in %r" % (name, value, actual)
    if 'overrideAffected' not in expected:
        assert actual.get('overrideAffected', False) is False, "reason must not be override-affected: %r" % actual


@pytest.mark.parametrize("vector", load_vectors(), ids=vector_id)
def test_override_spec_vector(vector: Dict[str, Any]):
    with make_client(vector) as client:
        assert client.is_initialized() is vector['launchDarklyData']['initialized']
        evaluate = vector['evaluate']
        detail = client.variation_detail(evaluate['flagKey'], Context.from_dict(evaluate['context']), evaluate['defaultValue'])

        expect = vector['expect']
        assert detail.value == expect['value'], "value"
        assert detail.variation_index == expect['variationIndex'], "variationIndex"
        assert_reason(expect['reason'], detail.reason)

        # summaryOverrideAffected is the marking the client hands to the event processor for this
        # evaluation. The event processor keys individual-event suppression and the summary
        # counter marker on that scalar, not on the reason.
        if 'summaryOverrideAffected' in expect:
            records = [e for e in client._event_processor._events if isinstance(e, EventInputEvaluation) and e.key == evaluate['flagKey']]
            assert len(records) == 1, "expected exactly one evaluation record for the flag"
            assert records[0].override_affected is expect['summaryOverrideAffected'], "summaryOverrideAffected"
