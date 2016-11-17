from ldclient.config import Config


def test_copy_config():
    old_sdk_key = "OLD_SDK_KEY"
    new_sdk_key = "NEW_SDK_KEY"

    old_config = Config(sdk_key=old_sdk_key, stream=False)

    assert old_config.sdk_key is old_sdk_key
    assert old_config.stream is False

    new_config = old_config.copy_with_new_sdk_key(new_sdk_key)
    assert new_config.sdk_key is new_sdk_key
    assert new_config.stream is False



