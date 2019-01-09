from collections import namedtuple

"""
These objects denote the types of data that can be stored in the feature store and
referenced in the API.  If we add another storable data type in the future, as long as it
follows the same pattern (having "key", "version", and "deleted" properties), we only need
to add a corresponding constant here and the existing store should be able to handle it.
"""

# Note that VersionedDataKind without the extra attributes is no longer used in the SDK,
# but it's preserved here for backward compatibility just in case someone else used it
VersionedDataKind = namedtuple('VersionedDataKind',
    ['namespace', 'request_api_path', 'stream_api_path'])

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
