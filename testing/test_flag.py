import pytest

from ldclient.feature_store import InMemoryFeatureStore
from ldclient.flag import evaluate
from ldclient.versioned_data_kind import SEGMENTS


def test_segment_match_clause_retrieves_segment_from_store():
    store = InMemoryFeatureStore()
    segment = {
        "key": "segkey",
        "included": [ "foo" ],
        "version": 1
    }
    store.upsert(SEGMENTS, segment)

    user = { "key": "foo" }
    flag = {
        "key": "test",
        "variations": [ False, True ],
        "fallthrough": { "variation": 0 },
        "on": True,
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "",
                        "op": "segmentMatch",
                        "values": [ "segkey" ]
                    }
                ],
                "variation": 1
            }
        ]
    }

    assert evaluate(flag, user, store) == (True, [])

def test_segment_match_clause_falls_through_with_no_errors_if_segment_not_found():
    store = InMemoryFeatureStore()

    user = { "key": "foo" }
    flag = {
        "key": "test",
        "variations": [ False, True ],
        "fallthrough": { "variation": 0 },
        "on": True,
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "",
                        "op": "segmentMatch",
                        "values": [ "segkey" ]
                    }
                ],
                "variation": 1
            }
        ]
    }

    assert evaluate(flag, user, store) == (False, [])
