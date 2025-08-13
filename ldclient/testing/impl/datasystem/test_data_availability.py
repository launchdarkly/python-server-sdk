import pytest

from ldclient.impl.datasystem import DataAvailability


def test_data_availability_enum_values():
    """Test that DataAvailability enum has the expected values."""
    assert DataAvailability.DEFAULTS == "defaults"
    assert DataAvailability.CACHED == "cached"
    assert DataAvailability.REFRESHED == "refreshed"


def test_data_availability_enum_type():
    """Test that DataAvailability is a string enum."""
    assert isinstance(DataAvailability.DEFAULTS, str)
    assert isinstance(DataAvailability.CACHED, str)
    assert isinstance(DataAvailability.REFRESHED, str)

    # Should also be instances of the enum class
    assert isinstance(DataAvailability.DEFAULTS, DataAvailability)
    assert isinstance(DataAvailability.CACHED, DataAvailability)
    assert isinstance(DataAvailability.REFRESHED, DataAvailability)


def test_at_least_same_value():
    """Test that at_least returns True when comparing the same value."""
    assert DataAvailability.DEFAULTS.at_least(DataAvailability.DEFAULTS) is True
    assert DataAvailability.CACHED.at_least(DataAvailability.CACHED) is True
    assert DataAvailability.REFRESHED.at_least(DataAvailability.REFRESHED) is True


def test_at_least_hierarchy():
    """Test the complete hierarchy of at_least relationships."""
    # DEFAULTS < CACHED < REFRESHED

    # DEFAULTS comparisons
    assert DataAvailability.DEFAULTS.at_least(DataAvailability.DEFAULTS) is True
    assert DataAvailability.DEFAULTS.at_least(DataAvailability.CACHED) is False
    assert DataAvailability.DEFAULTS.at_least(DataAvailability.REFRESHED) is False

    # CACHED comparisons
    assert DataAvailability.CACHED.at_least(DataAvailability.DEFAULTS) is True
    assert DataAvailability.CACHED.at_least(DataAvailability.CACHED) is True
    assert DataAvailability.CACHED.at_least(DataAvailability.REFRESHED) is False

    # REFRESHED comparisons
    assert DataAvailability.REFRESHED.at_least(DataAvailability.DEFAULTS) is True
    assert DataAvailability.REFRESHED.at_least(DataAvailability.CACHED) is True
    assert DataAvailability.REFRESHED.at_least(DataAvailability.REFRESHED) is True


def test_data_availability_string_operations():
    """Test that DataAvailability values work as strings."""
    defaults = DataAvailability.DEFAULTS
    cached = DataAvailability.CACHED
    refreshed = DataAvailability.REFRESHED

    # String concatenation
    assert defaults + "_test" == "defaults_test"
    assert cached + "_test" == "cached_test"
    assert refreshed + "_test" == "refreshed_test"

    # String formatting - need to use .value attribute for the actual string
    assert f"Status: {defaults.value}" == "Status: defaults"
    assert f"Status: {cached.value}" == "Status: cached"
    assert f"Status: {refreshed.value}" == "Status: refreshed"

    # String methods
    assert defaults.upper() == "DEFAULTS"
    assert cached.upper() == "CACHED"
    assert refreshed.upper() == "REFRESHED"


def test_data_availability_comparison_operators():
    """Test that DataAvailability values can be compared using standard operators."""
    # Equality
    assert DataAvailability.DEFAULTS == "defaults"
    assert DataAvailability.CACHED == "cached"
    assert DataAvailability.REFRESHED == "refreshed"

    # Inequality
    assert DataAvailability.DEFAULTS != "cached"
    assert DataAvailability.CACHED != "refreshed"
    assert DataAvailability.REFRESHED != "defaults"

    # String comparison (lexicographic) - 'cached' < 'defaults' < 'refreshed'
    assert DataAvailability.CACHED.value < DataAvailability.DEFAULTS.value
    assert DataAvailability.DEFAULTS.value < DataAvailability.REFRESHED.value
    assert DataAvailability.CACHED.value < DataAvailability.REFRESHED.value
