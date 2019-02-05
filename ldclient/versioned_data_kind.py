"""
This submodule is used only by the internals of the feature flag storage mechanism.

If you are writing your own implementation of :class:`ldclient.integrations.FeatureStore`, the
:class:`VersionedDataKind` tuple type will be passed to the ``kind`` parameter of the feature
store methods; its ``namespace`` property tells the feature store which collection of objects is
being referenced ("features", "segments", etc.). The intention is for the feature store to treat
storable objects as completely generic JSON dictionaries, rather than having any special logic
for features or segments.
"""

from collections import namedtuple

# Note that VersionedDataKind without the extra attributes is no longer used in the SDK,
# but it's preserved here for backward compatibility just in case someone else used it
VersionedDataKind = namedtuple('VersionedDataKind',
    ['namespace', 'request_api_path', 'stream_api_path'])

# Note, feature store implementors really don't need to know about this class so we could just
# not document it at all, but apparently namedtuple() creates its own docstrings so it's going
# to show up in any case.
VersionedDataKindWithOrdering = namedtuple('VersionedDataKindWithOrdering',
    ['namespace', 'request_api_path', 'stream_api_path', 'priority', 'get_dependency_keys'])

FEATURES = VersionedDataKindWithOrdering(namespace = "features",
    request_api_path = "/sdk/latest-flags",
    stream_api_path = "/flags/",
    priority = 1,
    get_dependency_keys = lambda flag: (p.get('key') for p in flag.get('prerequisites', [])))

SEGMENTS = VersionedDataKindWithOrdering(namespace = "segments",
    request_api_path = "/sdk/latest-segments",
    stream_api_path = "/segments/",
    priority = 0,
    get_dependency_keys = None)
