import logging

import pytest
from mock import patch

from ldclient.impl import util
from ldclient.impl.util import (
    _LD_FD_FALLBACK_HEADER,
    _LD_FD_FALLBACK_TTL_HEADER,
    fdv1_fallback_ttl,
    fdv1_fallback_ttl_from_headers,
    validate_sdk_key_format
)


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


@pytest.mark.parametrize("ttl, expected", [
    (1, 1.0),
    (0.5, 0.5),
    (3600, 3600.0),
    ("1", 1.0),
    ("3600", 3600.0),
])
def test_fdv1_fallback_ttl_uses_valid_ttl_as_is(ttl, expected):
    assert fdv1_fallback_ttl(ttl) == expected


@pytest.mark.parametrize("ttl", [None, 0, -1, 3601, "0", "-1", "3601", "", "abc", "1.5", True, {}])
def test_fdv1_fallback_ttl_jitters_default_for_invalid_ttl(ttl):
    assert 1800 <= fdv1_fallback_ttl(ttl) <= 3600


@pytest.mark.parametrize("jitter, expected", [(0, 3600.0), (1800, 1800.0)])
def test_fdv1_fallback_ttl_default_subtracts_up_to_half_an_hour(jitter, expected):
    with patch.object(util.random, "uniform", return_value=jitter) as uniform:
        assert fdv1_fallback_ttl(None) == expected

    uniform.assert_called_once_with(0, 1800)


def test_fdv1_fallback_ttl_from_headers():
    assert fdv1_fallback_ttl_from_headers({_LD_FD_FALLBACK_HEADER: 'true', _LD_FD_FALLBACK_TTL_HEADER: '30'}) == 30
    assert 1800 <= fdv1_fallback_ttl_from_headers({_LD_FD_FALLBACK_HEADER: 'true'}) <= 3600  # type: ignore[operator]
    assert fdv1_fallback_ttl_from_headers({_LD_FD_FALLBACK_TTL_HEADER: '30'}) is None
    assert fdv1_fallback_ttl_from_headers({}) is None
    assert fdv1_fallback_ttl_from_headers(None) is None
