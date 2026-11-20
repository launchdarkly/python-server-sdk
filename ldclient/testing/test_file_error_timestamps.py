import time

import pytest

from ldclient import LDClient
from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.integrations import Files
from ldclient.interfaces import DataSourceErrorKind


class FailingFeatureStore(InMemoryFeatureStore):
    def init(self, all_data):
        raise RuntimeError('store unavailable')


@pytest.mark.parametrize('failure', ['missing', 'malformed', 'store'])
def test_file_source_reports_numeric_error_timestamp(tmp_path, failure):
    path = tmp_path / 'flags.json'
    if failure == 'malformed':
        path.write_text('{"flagValues":{')
    elif failure == 'store':
        path.write_text('{"flagValues":{"flag":true}}')
    store = FailingFeatureStore() if failure == 'store' else InMemoryFeatureStore()
    started = time.time()
    with LDClient(config=Config(
        'SDK_KEY',
        update_processor_class=Files.new_data_source(paths=str(path)),
        feature_store=store,
        send_events=False,
    )) as client:
        error = client.data_source_status_provider.status.error
        assert error is not None
        assert error.kind == (DataSourceErrorKind.UNKNOWN if failure == 'store' else DataSourceErrorKind.INVALID_DATA)
        assert isinstance(error.time, float)
        assert started <= error.time <= time.time()
