"""
The flag and segment override layer. The layer is a runtime-mutable collection of flag and
segment definitions supplied by an override source. Those definitions take precedence over
LaunchDarkly data at evaluation time. Flag overrides are currently experimental and subject to
change.
"""

from ldclient.impl.overrides.layer import OverrideLayer
from ldclient.impl.overrides.overlay import (
    AsyncOverrideStoreView,
    OverrideStoreView
)
from ldclient.impl.overrides.sink import OverrideSinkImpl

__all__ = ['AsyncOverrideStoreView', 'OverrideLayer', 'OverrideSinkImpl', 'OverrideStoreView']
