from collections import namedtuple

"""
These objects denote the types of data that can be stored in the feature store and
referenced in the API.  If we add another storable data type in the future, as long as it
follows the same pattern (having "key", "version", and "deleted" properties), we only need
to add a corresponding constant here and the existing store should be able to handle it.
"""

VersionedDataKind = namedtuple('VersionedDataKind',
    ['namespace', 'request_api_path', 'stream_api_path'])

FEATURES = VersionedDataKind(namespace = "features",
    request_api_path = "/sdk/latest-flags",
    stream_api_path = "/flags/")

SEGMENTS = VersionedDataKind(namespace = "segments",
    request_api_path = "/sdk/latest-segments",
    stream_api_path = "/segments/")
