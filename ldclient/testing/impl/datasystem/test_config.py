import dataclasses
from unittest.mock import MagicMock, Mock

import pytest

from ldclient.config import Config as LDConfig
from ldclient.impl.datasystem import Initializer, Synchronizer
from ldclient.impl.datasystem.config import (
    Config,
    ConfigBuilder,
    custom,
    default,
    polling,
    streaming
)


def test_config_builder_initializers():
    """Test that initializers can be set and retrieved correctly."""
    builder = ConfigBuilder()
    mock_initializer = Mock()

    result = builder.initializers([mock_initializer])

    assert result is builder  # Method chaining
    assert builder._initializers == [mock_initializer]


def test_config_builder_synchronizers_primary_only():
    """Test that primary synchronizer can be set without secondary."""
    builder = ConfigBuilder()
    mock_synchronizer = Mock()

    result = builder.synchronizers(mock_synchronizer)

    assert result is builder  # Method chaining
    assert builder._primary_synchronizer == mock_synchronizer
    assert builder._secondary_synchronizer is None


def test_config_builder_synchronizers_with_secondary():
    """Test that both primary and secondary synchronizers can be set."""
    builder = ConfigBuilder()
    mock_primary = Mock()
    mock_secondary = Mock()

    result = builder.synchronizers(mock_primary, mock_secondary)

    assert result is builder  # Method chaining
    assert builder._primary_synchronizer == mock_primary
    assert builder._secondary_synchronizer == mock_secondary


def test_config_builder_build_success():
    """Test successful build with all required fields set."""
    builder = ConfigBuilder()
    mock_initializer = Mock()
    mock_primary = Mock()
    mock_secondary = Mock()

    builder.initializers([mock_initializer])
    builder.synchronizers(mock_primary, mock_secondary)

    config = builder.build()

    assert isinstance(config, Config)
    assert config.initializers == [mock_initializer]
    assert config.primary_synchronizer == mock_primary
    assert config.secondary_synchronizer == mock_secondary


def test_config_builder_build_missing_primary_synchronizer():
    """Test that build fails when primary synchronizer is not set."""
    builder = ConfigBuilder()

    with pytest.raises(ValueError, match="Primary synchronizer must be set"):
        builder.build()


def test_config_builder_build_with_initializers_only():
    """Test that build fails when only initializers are set."""
    builder = ConfigBuilder()
    mock_initializer = Mock()

    builder.initializers([mock_initializer])

    with pytest.raises(ValueError, match="Primary synchronizer must be set"):
        builder.build()


def test_config_builder_method_chaining():
    """Test that all builder methods support method chaining."""
    builder = ConfigBuilder()
    mock_initializer = Mock()
    mock_primary = Mock()
    mock_secondary = Mock()

    # Test that each method returns the builder instance
    result = builder.initializers([mock_initializer]).synchronizers(
        mock_primary, mock_secondary
    )

    assert result is builder


def test_config_builder_default_state():
    """Test that ConfigBuilder starts with all fields as None."""
    builder = ConfigBuilder()

    assert builder._initializers is None
    assert builder._primary_synchronizer is None
    assert builder._secondary_synchronizer is None


def test_config_builder_multiple_calls():
    """Test that multiple calls to builder methods overwrite previous values."""
    builder = ConfigBuilder()
    mock_initializer1 = Mock()
    mock_initializer2 = Mock()
    mock_primary1 = Mock()
    mock_primary2 = Mock()

    # Set initial values
    builder.initializers([mock_initializer1])
    builder.synchronizers(mock_primary1)

    # Overwrite with new values
    builder.initializers([mock_initializer2])
    builder.synchronizers(mock_primary2)

    config = builder.build()

    assert config.initializers == [mock_initializer2]
    assert config.primary_synchronizer == mock_primary2


def test_custom_builder():
    """Test that custom() returns a fresh ConfigBuilder instance."""
    builder1 = custom()
    builder2 = custom()

    assert isinstance(builder1, ConfigBuilder)
    assert isinstance(builder2, ConfigBuilder)
    assert builder1 is not builder2  # Different instances


def test_default_config_builder():
    """Test that default() returns a properly configured ConfigBuilder."""
    mock_ld_config = Mock(spec=LDConfig)

    builder = default(mock_ld_config)

    assert isinstance(builder, ConfigBuilder)
    # The actual implementation details would be tested in integration tests
    # Here we just verify it returns a builder


def test_streaming_config_builder():
    """Test that streaming() returns a properly configured ConfigBuilder."""
    mock_ld_config = Mock(spec=LDConfig)

    builder = streaming(mock_ld_config)

    assert isinstance(builder, ConfigBuilder)
    # The actual implementation details would be tested in integration tests
    # Here we just verify it returns a builder


def test_polling_config_builder():
    """Test that polling() returns a properly configured ConfigBuilder."""
    mock_ld_config = Mock(spec=LDConfig)

    builder = polling(mock_ld_config)

    assert isinstance(builder, ConfigBuilder)
    # The actual implementation details would be tested in integration tests
    # Here we just verify it returns a builder


def test_config_dataclass_immutability():
    """Test that Config instances are immutable (frozen dataclass)."""
    mock_primary = Mock()
    mock_secondary = Mock()

    config = Config(
        initializers=None,
        primary_synchronizer=mock_primary,
        secondary_synchronizer=mock_secondary,
    )

    # Attempting to modify attributes should raise an error
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.primary_synchronizer = Mock()


def test_config_builder_with_none_initializers():
    """Test that initializers can be explicitly set to None."""
    builder = ConfigBuilder()
    mock_primary = Mock()

    builder.initializers(None)
    builder.synchronizers(mock_primary)

    config = builder.build()

    assert config.initializers is None
    assert config.primary_synchronizer == mock_primary


def test_config_builder_with_empty_initializers_list():
    """Test that empty list of initializers is handled correctly."""
    builder = ConfigBuilder()
    mock_primary = Mock()

    builder.initializers([])
    builder.synchronizers(mock_primary)

    config = builder.build()

    assert config.initializers == []
    assert config.primary_synchronizer == mock_primary
