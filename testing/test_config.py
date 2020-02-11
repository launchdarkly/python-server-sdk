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

def test_can_set_valid_poll_interval():
    config = Config(sdk_key = "SDK_KEY", poll_interval = 31)
    assert config.poll_interval == 31

def test_minimum_poll_interval_is_enforced():
    config = Config(sdk_key = "SDK_KEY", poll_interval = 29)
    assert config.poll_interval == 30

def test_can_set_valid_diagnostic_interval():
    config = Config(sdk_key = "SDK_KEY", diagnostic_recording_interval=61)
    assert config.diagnostic_recording_interval == 61

def test_minimum_diagnostic_interval_is_enforced():
    config = Config(sdk_key = "SDK_KEY", diagnostic_recording_interval=59)
    assert config.diagnostic_recording_interval == 60
