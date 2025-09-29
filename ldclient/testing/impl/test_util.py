import logging
from unittest.mock import Mock
from ldclient.impl.util import validate_sdk_key


def test_validate_sdk_key_valid():
    """Test validation of valid SDK keys"""
    logger = Mock(spec=logging.Logger)
    
    valid_keys = [
        "sdk-12345678-1234-1234-1234-123456789012",
        "valid-sdk-key-123",
        "VALID_SDK_KEY_456"
    ]
    
    for key in valid_keys:
        assert validate_sdk_key(key, logger) is True
        logger.warning.assert_not_called()
        logger.reset_mock()


def test_validate_sdk_key_invalid():
    """Test validation of invalid SDK keys"""
    logger = Mock(spec=logging.Logger)
    
    invalid_keys = [
        "sdk-key-with-\x00-null",
        "sdk-key-with-\n-newline", 
        "sdk-key-with-\t-tab"
    ]
    
    for key in invalid_keys:
        assert validate_sdk_key(key, logger) is False
        logger.warning.assert_called_with("SDK key contains invalid characters")
        logger.reset_mock()


def test_validate_sdk_key_non_string():
    """Test validation of non-string SDK keys"""
    logger = Mock(spec=logging.Logger)
    
    assert validate_sdk_key("123", logger) is True
    logger.warning.assert_not_called()


def test_validate_sdk_key_empty():
    """Test validation of empty SDK keys"""
    logger = Mock(spec=logging.Logger)
    
    assert validate_sdk_key("", logger) is True
    logger.warning.assert_not_called()
