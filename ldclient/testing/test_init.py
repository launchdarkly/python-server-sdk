import logging
from pprint import pprint

import ldclient
from ldclient import Config

mylogger = logging.getLogger()


def test_set_config():
    old_sdk_key = "OLD_SDK_KEY"
    new_sdk_key = "NEW_SDK_KEY"

    old_config = Config(sdk_key=old_sdk_key, stream=False, offline=True)
    new_config = Config(sdk_key=new_sdk_key, stream=False, offline=True)
    ldclient.set_config(old_config)

    old_client = ldclient.get()
    assert old_client.get_sdk_key() == old_sdk_key

    ldclient.set_config(new_config)
    new_client = ldclient.get()

    assert new_client.get_sdk_key() == new_sdk_key

    # illustrates bad behavior- assigning value of ldclient.get() means
    # the old_client didn't get updated when we called set_config()
    assert old_client.get_sdk_key() == old_sdk_key
