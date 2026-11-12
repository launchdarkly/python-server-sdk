"""
The FDv2 polling builders apply the same 30 second minimum poll interval as ``Config``. This
includes an interval that is set directly on a builder.
"""

import pytest

from ldclient.async_config import AsyncConfig
from ldclient.config import Config
from ldclient.impl.datasource.async_polling import (
    AsyncFallbackToFDv1PollingDataSourceBuilder,
    AsyncPollingDataSourceBuilder
)
from ldclient.impl.datasource.polling import (
    FallbackToFDv1PollingDataSourceBuilder,
    PollingDataSourceBuilder
)

SYNC_BUILDERS = [PollingDataSourceBuilder, FallbackToFDv1PollingDataSourceBuilder]
ASYNC_BUILDERS = [AsyncPollingDataSourceBuilder, AsyncFallbackToFDv1PollingDataSourceBuilder]


@pytest.mark.parametrize('builder_class', SYNC_BUILDERS)
@pytest.mark.parametrize('interval, expected', [(1, 30), (30, 30), (45, 45)])
def test_builder_interval_has_a_minimum(builder_class, interval, expected):
    source = builder_class().poll_interval(interval).build(Config('sdk-key'))
    assert source._poll_interval == expected


@pytest.mark.parametrize('builder_class', SYNC_BUILDERS)
def test_builder_uses_the_config_interval_when_none_is_set(builder_class):
    source = builder_class().build(Config('sdk-key', poll_interval=60))
    assert source._poll_interval == 60


@pytest.mark.parametrize('builder_class', ASYNC_BUILDERS)
@pytest.mark.parametrize('interval, expected', [(1, 30), (30, 30), (45, 45)])
def test_async_builder_interval_has_a_minimum(builder_class, interval, expected):
    source = builder_class().poll_interval(interval).build(AsyncConfig('sdk-key'))
    assert source._poll_interval == expected


@pytest.mark.parametrize('builder_class', ASYNC_BUILDERS)
def test_async_builder_uses_the_config_interval_when_none_is_set(builder_class):
    source = builder_class().build(AsyncConfig('sdk-key', poll_interval=60))
    assert source._poll_interval == 60
