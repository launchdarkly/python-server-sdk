import logging

from ldclient.impl.util import validate_sdk_key_format


def test_validate_sdk_key_format_valid():
    """Test validation of valid SDK keys"""
    logger = logging.getLogger('test')
    valid_keys = [
        "sdk-12345678-1234-1234-1234-123456789012",
        "valid-sdk-key-123",
        "VALID_SDK_KEY_456",
        "test.key_with.dots",
        "test-key-with-hyphens"
    ]

    for key in valid_keys:
        result = validate_sdk_key_format(key, logger)
        assert result == key  # Should return the same key if valid


def test_validate_sdk_key_format_invalid():
    """Test validation of invalid SDK keys"""
    logger = logging.getLogger('test')
    invalid_keys = [
        "sdk-key-with-\x00-null",
        "sdk-key-with-\n-newline",
        "sdk-key-with-\t-tab",
        "sdk key with spaces",
        "sdk@key#with$special%chars",
        "sdk/key\\with/slashes"
    ]

    for key in invalid_keys:
        result = validate_sdk_key_format(key, logger)
        assert result == ''  # Should return empty string for invalid keys


def test_validate_sdk_key_format_non_string():
    """Test validation of non-string SDK keys"""
    logger = logging.getLogger('test')
    non_string_values = [123, object(), [], {}]

    for value in non_string_values:
        result = validate_sdk_key_format(value, logger)
        assert result == ''  # Should return empty string for non-string values


def test_validate_sdk_key_format_empty_and_none():
    """Test validation of empty and None SDK keys"""
    logger = logging.getLogger('test')
    assert validate_sdk_key_format("", logger) == ''  # Empty string should return empty string
    assert validate_sdk_key_format(None, logger) == ''  # None should return empty string


def test_validate_sdk_key_format_max_length():
    """Test validation of SDK key maximum length"""
    logger = logging.getLogger('test')
    valid_key = "a" * 8192
    result = validate_sdk_key_format(valid_key, logger)
    assert result == valid_key  # Should return the same key if valid

    invalid_key = "a" * 8193
    result = validate_sdk_key_format(invalid_key, logger)
    assert result == ''  # Should return empty string for keys that are too long
