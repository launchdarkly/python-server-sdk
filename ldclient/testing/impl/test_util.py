from ldclient.impl.util import is_valid_sdk_key_format


def test_is_valid_sdk_key_format_valid():
    """Test validation of valid SDK keys"""
    valid_keys = [
        "sdk-12345678-1234-1234-1234-123456789012",
        "valid-sdk-key-123",
        "VALID_SDK_KEY_456",
        "test.key_with.dots",
        "test-key-with-hyphens"
    ]
    
    for key in valid_keys:
        assert is_valid_sdk_key_format(key) is True


def test_is_valid_sdk_key_format_invalid():
    """Test validation of invalid SDK keys"""
    invalid_keys = [
        "sdk-key-with-\x00-null",
        "sdk-key-with-\n-newline", 
        "sdk-key-with-\t-tab",
        "sdk key with spaces",
        "sdk@key#with$special%chars",
        "sdk/key\\with/slashes"
    ]
    
    for key in invalid_keys:
        assert is_valid_sdk_key_format(key) is False


def test_is_valid_sdk_key_format_non_string():
    """Test validation of non-string SDK keys"""
    non_string_values = [123, object(), [], {}]
    
    for value in non_string_values:
        assert is_valid_sdk_key_format(value) is False


def test_is_valid_sdk_key_format_empty_and_none():
    """Test validation of empty and None SDK keys"""
    assert is_valid_sdk_key_format("") is True
    assert is_valid_sdk_key_format(None) is True


def test_is_valid_sdk_key_format_max_length():
    """Test validation of SDK key maximum length"""
    valid_key = "a" * 8192
    assert is_valid_sdk_key_format(valid_key) is True
    
    invalid_key = "a" * 8193
    assert is_valid_sdk_key_format(invalid_key) is False
