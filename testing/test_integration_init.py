import pytest
import ldclient
from ldclient import Config
import os
from testing.sync_util import wait_until

import logging

sdk_key = os.environ.get('LD_SDK_KEY')

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.skipif(sdk_key is None, reason="requires LD_SDK_KEY environment variable to be set")
def test_set_sdk_key():
    ldclient.set_config(Config.default())
    assert ldclient.get().is_initialized() is False
    ldclient.set_sdk_key(sdk_key)
    wait_until(ldclient.get().is_initialized, timeout=10)

    ldclient.get().close()


@pytest.mark.skipif(sdk_key is None, reason="requires LD_SDK_KEY environment variable to be set")
def test_set_config():
    offline_config = ldclient.Config(offline=True)
    online_config = ldclient.Config(sdk_key=sdk_key, offline=False)

    ldclient.set_config(offline_config)
    assert ldclient.get().is_offline() is True

    ldclient.set_config(online_config)
    assert ldclient.get().is_offline() is False
    wait_until(ldclient.get().is_initialized, timeout=10)

    ldclient.get().close()

