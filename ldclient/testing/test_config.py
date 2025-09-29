import pytest

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
    config = Config(sdk_key="SDK_KEY", poll_interval=31)
    assert config.poll_interval == 31


def test_minimum_poll_interval_is_enforced():
    config = Config(sdk_key="SDK_KEY", poll_interval=29)
    assert config.poll_interval == 30


def test_can_set_valid_diagnostic_interval():
    config = Config(sdk_key="SDK_KEY", diagnostic_recording_interval=61)
    assert config.diagnostic_recording_interval == 61


def test_minimum_diagnostic_interval_is_enforced():
    config = Config(sdk_key="SDK_KEY", diagnostic_recording_interval=59)
    assert config.diagnostic_recording_interval == 60


def test_trims_trailing_slashes_on_uris():
    config = Config(sdk_key="SDK_KEY", base_uri="https://launchdarkly.com/", events_uri="https://docs.launchdarkly.com/", stream_uri="https://blog.launchdarkly.com/")

    assert config.base_uri == "https://launchdarkly.com"
    assert config.events_uri == "https://docs.launchdarkly.com/bulk"
    assert config.stream_base_uri == "https://blog.launchdarkly.com"


def test_sdk_key_validation_valid_keys():
    """Test that valid SDK keys are accepted"""
    valid_keys = [
        "sdk-12345678-1234-1234-1234-123456789012",
        "valid-sdk-key-123",
        "VALID_SDK_KEY_456",
        "test.key_with.dots",
        "test-key-with-hyphens"
    ]

    for key in valid_keys:
        config = Config(sdk_key=key)
        assert config.sdk_key == key


def test_sdk_key_validation_invalid_keys():
    """Test that invalid SDK keys are not set"""
    invalid_keys = [
        "sdk-key-with-\x00-null",
        "sdk-key-with-\n-newline",
        "sdk-key-with-\t-tab",
        "sdk key with spaces",
        "sdk@key#with$special%chars",
        "sdk/key\\with/slashes"
    ]

    for key in invalid_keys:
        config = Config(sdk_key=key)
        assert config.sdk_key == ''


def test_sdk_key_validation_empty_key():
    """Test that empty SDK keys are accepted"""
    config = Config(sdk_key="")
    assert config.sdk_key == ""


def test_sdk_key_validation_none_key():
    """Test that None SDK keys are accepted"""
    config = Config(sdk_key=None)
    assert config.sdk_key == ''


def test_sdk_key_validation_max_length():
    """Test SDK key maximum length validation"""
    valid_key = "a" * 8192
    config = Config(sdk_key=valid_key)
    assert config.sdk_key == valid_key

    invalid_key = "a" * 8193
    config = Config(sdk_key=invalid_key)
    assert config.sdk_key == ''


def test_copy_with_new_sdk_key_validation():
    """Test that copy_with_new_sdk_key validates the new key"""
    original_config = Config(sdk_key="valid-key")

    new_config = original_config.copy_with_new_sdk_key("another-valid-key")
    assert new_config.sdk_key == "another-valid-key"

    invalid_config = original_config.copy_with_new_sdk_key("invalid key with spaces")
    assert invalid_config.sdk_key == ''


def application_can_be_set_and_read():
    application = {"id": "my-id", "version": "abcdef"}
    config = Config(sdk_key="SDK_KEY", application=application)
    assert config.application == {"id": "my-id", "version": "abcdef"}


def application_can_handle_non_string_values():
    application = {"id": 1, "version": 2}
    config = Config(sdk_key="SDK_KEY", application=application)
    assert config.application == {"id": "1", "version": "2"}


def application_will_ignore_invalid_keys():
    application = {"invalid": 1, "key": 2}
    config = Config(sdk_key="SDK_KEY", application=application)
    assert config.application == {"id": "", "version": ""}


@pytest.fixture(params=[" ", "@", ":", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-a"])
def invalid_application_tags(request):
    return request.param


def test_application_will_drop_invalid_values(invalid_application_tags):
    application = {"id": invalid_application_tags, "version": invalid_application_tags}
    config = Config(sdk_key="SDK_KEY", application=application)
    assert config.application == {"id": "", "version": ""}
