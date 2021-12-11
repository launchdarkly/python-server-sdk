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
from typing import Callable, Iterable, Optional

# Note that VersionedDataKind without the extra attributes is no longer used in the SDK,
# but it's preserved here for backward compatibility just in case someone else used it
class VersionedDataKind:
    def __init__(self, namespace: str, request_api_path: str, stream_api_path: str):
        self._namespace = namespace
        self._request_api_path = request_api_path
        self._stream_api_path = stream_api_path

    @property
    def namespace(self) -> str:
        return self._namespace
    
    @property
    def request_api_path(self) -> str:
        return self._request_api_path
    
    @property
    def stream_api_path(self) -> str:
        return self._stream_api_path

class VersionedDataKindWithOrdering(VersionedDataKind):
    def __init__(self, namespace: str, request_api_path: str, stream_api_path: str,
                 priority: int, get_dependency_keys: Optional[Callable[[dict], Iterable[str]]]):
        super().__init__(namespace, request_api_path, stream_api_path)
        self._priority = priority
        self._get_dependency_keys = get_dependency_keys
    
    @property
    def priority(self) -> int:
        return self._priority
    
    @property
    def get_dependency_keys(self) -> Optional[Callable[[dict], Iterable[str]]]:
        return self._get_dependency_keys

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
