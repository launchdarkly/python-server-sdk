# pylint: disable=missing-docstring

from threading import Event

from ldclient.config import Config
from ldclient.feature_store import InMemoryFeatureStore
from ldclient.impl.datasystem import DataAvailability
from ldclient.impl.datasystem.fdv1 import FDv1
from ldclient.versioned_data_kind import FEATURES


def test_fdv1_availability_offline():
    """Test that FDv1 returns DEFAULTS for both data and target availability when offline."""
    config = Config(sdk_key="sdk-key", offline=True)
    fdv1 = FDv1(config)

    assert fdv1.data_availability == DataAvailability.DEFAULTS
    assert fdv1.target_availability == DataAvailability.DEFAULTS


def test_fdv1_availability_ldd_mode_uninitialized():
    """Test that FDv1 returns DEFAULTS for data and CACHED for target when LDD mode with uninitialized store."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key="sdk-key", use_ldd=True, feature_store=store)
    fdv1 = FDv1(config)

    # Store is not initialized yet
    assert not store.initialized
    assert fdv1.data_availability == DataAvailability.DEFAULTS
    assert fdv1.target_availability == DataAvailability.CACHED


def test_fdv1_availability_ldd_mode_initialized():
    """Test that FDv1 returns CACHED for both when LDD mode with initialized store."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key="sdk-key", use_ldd=True, feature_store=store)
    fdv1 = FDv1(config)

    # Initialize the store
    store.init({FEATURES: {}})

    assert store.initialized
    assert fdv1.data_availability == DataAvailability.CACHED
    assert fdv1.target_availability == DataAvailability.CACHED


def test_fdv1_availability_normal_mode_uninitialized():
    """Test that FDv1 returns DEFAULTS for data and REFRESHED for target in normal mode when not initialized."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key="sdk-key", feature_store=store)
    fdv1 = FDv1(config)

    # Update processor not started, store not initialized
    assert fdv1.data_availability == DataAvailability.DEFAULTS
    assert fdv1.target_availability == DataAvailability.REFRESHED


def test_fdv1_availability_normal_mode_store_initialized():
    """Test that FDv1 returns CACHED for data and REFRESHED for target when store is initialized but update processor is not."""
    store = InMemoryFeatureStore()
    config = Config(sdk_key="sdk-key", feature_store=store)
    fdv1 = FDv1(config)

    # Initialize store but don't start update processor
    fdv1._store_wrapper.init({FEATURES: {}})

    assert fdv1.data_availability == DataAvailability.CACHED
    assert fdv1.target_availability == DataAvailability.REFRESHED
