import logging
import sys

import pytest

from ldclient import Config
from ldclient import LDClient
from testing import sdk_key
from testing.sync_util import wait_until

logging.basicConfig(level=logging.DEBUG)


# skipping for Python 2.6 since it is incompatible with LaunchDarkly's streaming connection due to SNI
@pytest.mark.skipif(sdk_key is None or sys.version_info < (2, 7),
                    reason="Requires Python >=2.7 and LD_SDK_KEY environment variable to be set")
def test_ctor_with_sdk_key():
    client = LDClient(sdk_key=sdk_key)
    wait_until(client.is_initialized, timeout=10)

    client.close()


# skipping for Python 2.6 since it is incompatible with LaunchDarkly's streaming connection due to SNI
@pytest.mark.skipif(sdk_key is None or sys.version_info < (2, 7),
                    reason="Requires Python >=2.7 and LD_SDK_KEY environment variable to be set")
def test_ctor_with_sdk_key_and_config():
    client = LDClient(sdk_key=sdk_key, config=Config.default())
    wait_until(client.is_initialized, timeout=10)

    client.close()


# skipping for Python 2.6 since it is incompatible with LaunchDarkly's streaming connection due to SNI
@pytest.mark.skipif(sdk_key is None or sys.version_info < (2, 7),
                    reason="Requires Python >=2.7 and LD_SDK_KEY environment variable to be set")
def test_ctor_with_config():
    client = LDClient(config=Config(sdk_key=sdk_key))
    wait_until(client.is_initialized, timeout=10)

    client.close()


#polling
@pytest.mark.skipif(sdk_key is None,
                    reason="requires LD_SDK_KEY environment variable to be set")
def test_ctor_with_config_polling():
    client = LDClient(config=Config(sdk_key=sdk_key, stream=False))
    wait_until(client.is_initialized, timeout=10)

    client.close()
