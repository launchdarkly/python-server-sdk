import logging

import ldclient
from ldclient import Config

logging.basicConfig(level=logging.DEBUG)
mylogger = logging.getLogger()


def test_init():
    old_sdk_key = "OLD_SDK_KEY"
    new_sdk_key = "NEW_SDK_KEY"

    old_config = Config(sdk_key=old_sdk_key, stream=False, offline=True)
    ldclient.set_config(old_config)

    old_client = ldclient.get()
    assert old_client.get_sdk_key() == old_sdk_key

    ldclient.set_sdk_key(new_sdk_key)
    new_client = ldclient.get()


    print("old client: " + str(old_client))
    print("new client: " + str(new_client))
    assert new_client.get_sdk_key() == new_sdk_key

    # print(old_client)
    assert old_client.get_sdk_key() == new_sdk_key



