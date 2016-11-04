import pytest
import ldclient
import os
from sync_util import wait_until

import logging

sdk_key = os.environ.get('LD_SDK_KEY')

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.skipif(sdk_key is None, reason="requires LD_SDK_KEY environment variable to be set")
def test_set_sdk_key_singleton():
    client = ldclient.get()
    assert client.is_initialized() is False
    wait_until(ldclient.get(sdk_key).is_initialized, timeout=10)


@pytest.mark.skipif(sdk_key is None, reason="requires LD_SDK_KEY environment variable to be set")
def test_set_sdk_key():
    client = ldclient.LDClient()
    assert client.is_initialized() is False
    client.set_sdk_key(sdk_key)
    wait_until(client.is_initialized, timeout=10)

