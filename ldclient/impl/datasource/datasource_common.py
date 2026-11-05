"""
Pure helpers shared by the FDv1 polling and streaming data sources.
"""

# currently excluded from documentation - see docs/README.md

from collections import namedtuple
from typing import Mapping, Optional, Protocol, TypeVar, runtime_checkable

from ldclient.impl.util import _LD_ENVID_HEADER
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

STREAM_ALL_PATH = '/all'
FDV1_POLLING_ENDPOINT = '/sdk/latest-all'

T = TypeVar('T')

ParsedPath = namedtuple('ParsedPath', ['kind', 'key'])


class StreamClosedError(Exception):
    """The stream connection closed cleanly, and the SDK did not ask for it.

    The service normally leaves the connection open, so a close the SDK did
    not ask for is a connection failure. The SDK backs off before it
    reconnects, rather than reconnecting at once.

    It is a NORMAL failure, not an UNEXPECTED one. A load balancer draining
    during a rolling deploy closes streams cleanly, and putting that in the
    extended regime would take a whole fleet out of service for up to an
    hour.
    """

    def __init__(self):
        super().__init__("the server closed the stream connection")


def require_update_sink(sink: Optional[T]) -> T:
    """
    Returns the data source update sink, or raises if it is missing.

    The data system puts the sink on the config before it builds a data
    source. A data source writes all its updates to the sink, so it cannot
    operate without one.

    :raises ValueError: if the sink is None
    """
    if sink is None:
        raise ValueError("config.data_source_update_sink is not set. The data system must set it before it builds a data source.")

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
