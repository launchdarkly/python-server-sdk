import time
from typing import Tuple

import pytest
from mock import Mock

from ldclient.impl.datasystem import fdv2_common
from ldclient.impl.datasystem.fdv2_common import (
    DataSourceStatusProviderImpl,
    DataStoreStatusProviderImpl
)
from ldclient.impl.listeners import Listeners
from ldclient.interfaces import (
    DataSourceErrorInfo,
    DataSourceErrorKind,
    DataSourceState,
    DataStoreStatus
)
from ldclient.testing.test_util import SpyListener


def make_error(message: str = 'boom') -> DataSourceErrorInfo:
    return DataSourceErrorInfo(DataSourceErrorKind.NETWORK_ERROR, 0, time.time(), message)


def make_source_provider() -> Tuple[DataSourceStatusProviderImpl, SpyListener]:
    spy = SpyListener()
    provider = DataSourceStatusProviderImpl(Listeners())
    provider.add_listener(spy)
    return provider, spy


def make_store_provider(store=None) -> Tuple[DataStoreStatusProviderImpl, SpyListener]:
    spy = SpyListener()
    provider = DataStoreStatusProviderImpl(store, Listeners())
    provider.add_listener(spy)
    return provider, spy


def test_data_source_status_defaults_to_initializing_without_error():
    provider, _ = make_source_provider()

    assert provider.status.state == DataSourceState.INITIALIZING
    assert provider.status.error is None


def test_interrupted_while_initializing_stays_initializing():
    provider, spy = make_source_provider()

    provider.update_status(DataSourceState.INTERRUPTED, None)

    assert provider.status.state == DataSourceState.INITIALIZING
    assert provider.status.error is None
    assert spy.statuses == []


def test_interrupted_with_error_while_initializing_reports_initializing_with_error():
    provider, spy = make_source_provider()
    error = make_error()

    provider.update_status(DataSourceState.INTERRUPTED, error)

    assert provider.status.state == DataSourceState.INITIALIZING
    assert provider.status.error is error
    assert len(spy.statuses) == 1


def test_state_change_notifies_listener_once():
    provider, spy = make_source_provider()

    provider.update_status(DataSourceState.VALID, None)

    assert len(spy.statuses) == 1
    assert spy.statuses[0].state == DataSourceState.VALID


def test_same_state_without_error_does_not_notify_again():
    provider, spy = make_source_provider()

    provider.update_status(DataSourceState.VALID, None)
    provider.update_status(DataSourceState.VALID, None)
    assert len(spy.statuses) == 1

    provider.update_status(DataSourceState.INTERRUPTED, None)
    provider.update_status(DataSourceState.INTERRUPTED, None)
    assert len(spy.statuses) == 2


def test_same_state_with_new_error_notifies_and_keeps_since():
    provider, spy = make_source_provider()
    provider.update_status(DataSourceState.VALID, None)
    since = provider.status.since
    error = make_error()

    provider.update_status(DataSourceState.VALID, error)

    assert len(spy.statuses) == 2
    assert spy.statuses[1].error is error
    assert provider.status.since == since


def test_state_change_updates_since(monkeypatch):
    provider, _ = make_source_provider()
    monkeypatch.setattr(fdv2_common.time, 'time', lambda: 1000.0)
    provider.update_status(DataSourceState.VALID, None)
    assert provider.status.since == 1000.0

    monkeypatch.setattr(fdv2_common.time, 'time', lambda: 2000.0)
    provider.update_status(DataSourceState.INTERRUPTED, None)

    assert provider.status.since == 2000.0


def test_error_is_kept_when_later_update_has_no_error():
    provider, _ = make_source_provider()
    error = make_error()
    provider.update_status(DataSourceState.VALID, None)
    provider.update_status(DataSourceState.INTERRUPTED, error)

    provider.update_status(DataSourceState.VALID, None)

    assert provider.status.state == DataSourceState.VALID
    assert provider.status.error is error


def test_all_listeners_are_notified_for_a_single_change():
    provider, spy1 = make_source_provider()
    spy2 = SpyListener()
    provider.add_listener(spy2)

    provider.update_status(DataSourceState.VALID, None)

    assert len(spy1.statuses) == 1
    assert len(spy2.statuses) == 1


def test_removed_data_source_listener_is_not_notified():
    provider, spy = make_source_provider()
    provider.update_status(DataSourceState.VALID, None)

    provider.remove_listener(spy)
    provider.update_status(DataSourceState.INTERRUPTED, None)

    assert len(spy.statuses) == 1


@pytest.mark.parametrize('later_state', [DataSourceState.VALID, DataSourceState.INTERRUPTED, DataSourceState.INITIALIZING])
def test_off_is_terminal(later_state):
    provider, spy = make_source_provider()
    provider.update_status(DataSourceState.VALID, None)
    provider.update_status(DataSourceState.OFF, None)

    provider.update_status(later_state, make_error())

    assert provider.status.state == DataSourceState.OFF
    assert provider.status.error is None
    assert [s.state for s in spy.statuses] == [DataSourceState.VALID, DataSourceState.OFF]


def test_data_store_status_defaults_to_available_and_not_stale():
    provider, _ = make_store_provider()

    assert provider.status == DataStoreStatus(True, False)


def test_data_store_status_change_notifies_and_updates_status():
    provider, spy = make_store_provider()
    unavailable = DataStoreStatus(False, False)

    provider.update_status(unavailable)

    assert provider.status == unavailable
    assert spy.statuses == [unavailable]


def test_data_store_unchanged_status_does_not_notify():
    provider, spy = make_store_provider()

    provider.update_status(DataStoreStatus(True, False))
    assert spy.statuses == []

    provider.update_status(DataStoreStatus(False, False))
    provider.update_status(DataStoreStatus(False, False))
    assert len(spy.statuses) == 1


def test_data_store_status_returns_a_copy():
    provider, _ = make_store_provider()
    provider.update_status(DataStoreStatus(False, True))

    first = provider.status
    second = provider.status

    assert first == second
    assert first is not second


def test_removed_data_store_listener_is_not_notified():
    provider, spy = make_store_provider()
    provider.update_status(DataStoreStatus(False, False))

    provider.remove_listener(spy)
    provider.update_status(DataStoreStatus(True, True))

    assert len(spy.statuses) == 1


def test_monitoring_is_disabled_without_a_store():
    provider, _ = make_store_provider(None)

    assert not provider.is_monitoring_enabled()


def test_monitoring_delegates_to_the_store():
    store = Mock()
    store.is_monitoring_enabled.return_value = True
    provider, _ = make_store_provider(store)

    assert provider.is_monitoring_enabled()

    store.is_monitoring_enabled.return_value = False
    assert not provider.is_monitoring_enabled()
