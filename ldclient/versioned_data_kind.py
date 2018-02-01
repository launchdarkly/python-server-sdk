

"""
These objects denote the types of data that can be stored in the feature store and
referenced in the API.  If we add another storable data type in the future, as long as it
follows the same pattern (having "key", "version", and "deleted" properties), we only need
to add a corresponding constant here and the existing store should be able to handle it.
"""

class VersionedDataKind(object):
    def __init__(self, namespace, request_api_path, stream_api_path):
        self.__namespace = namespace
        self.__request_api_path = request_api_path
        self.__stream_api_path = stream_api_path

    @property
    def namespace(self):
        return self.__namespace

    @property
    def request_api_path(self):
        return self.__request_api_path
    
    @property
    def stream_api_path(self):
        return self.__stream_api_path

FEATURES = VersionedDataKind("features",
    "/sdk/latest-flags",
    "/flags/")

SEGMENTS = VersionedDataKind("segments",
    "/sdk/latest-segments",
    "/segments/")
