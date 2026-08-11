"""
Pure helpers shared by the FDv1 polling and streaming data sources.
"""

# currently excluded from documentation - see docs/README.md

from collections import namedtuple
from typing import Mapping, Optional, Protocol, runtime_checkable

from ldclient.impl.util import _LD_ENVID_HEADER
from ldclient.interfaces import DataSourceUpdateSink, FeatureStore
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

STREAM_ALL_PATH = '/all'
FDV1_POLLING_ENDPOINT = '/sdk/latest-all'

ParsedPath = namedtuple('ParsedPath', ['kind', 'key'])


def sink_or_store(sink: Optional[DataSourceUpdateSink], store: FeatureStore):
    """
    The original implementation of the data sources relied on the feature store
    directly, which we are trying to move away from. Customers who might have
    instantiated one directly for some reason wouldn't know they have to set
    the config's sink manually, so we have to fall back to the store if the
    sink isn't present.

    The next major release should be able to simplify this structure and
    remove the need for fall back to the data store because the update sink
    should always be present.
    """
    if sink is None:
        return store

    return sink


@runtime_checkable
class EnvironmentIdSink(Protocol):
    """
    Implemented by data source update sinks which can record the environment ID
    reported by LaunchDarkly. This is separate from
    :class:`ldclient.interfaces.DataSourceUpdateSink` so that externally
    implemented sinks remain compatible.
    """

    def set_environment_id(self, environment_id: str) -> None:
        ...


def record_environment_id(sink, headers: Optional[Mapping[str, str]]):
    """
    Records the environment ID from a set of LaunchDarkly response headers, if
    both the headers and the sink provide one.
    """
    if headers is None or not isinstance(sink, EnvironmentIdSink):
        return

    environment_id = headers.get(_LD_ENVID_HEADER)
    if isinstance(environment_id, str) and environment_id != '':
        sink.set_environment_id(environment_id)


def parse_path(path: str):
    for kind in [FEATURES, SEGMENTS]:
        if path.startswith(kind.stream_api_path):
            return ParsedPath(kind=kind, key=path[len(kind.stream_api_path):])
    return None
