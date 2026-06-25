# pylint: disable=missing-docstring
from unittest import mock

from ldclient.impl.datasourcev2.streaming import _close_pool_manager


def test_closes_each_connection_pool_then_clears():
    # _close_pool_manager must close every pooled HTTPConnectionPool (sending the FIN
    # the server waits on for the FDv1 Fallback Directive) and then clear the
    # PoolManager. SSEClient.close() alone only releases the connection.
    cp1 = mock.Mock()
    cp2 = mock.Mock()
    pool = mock.Mock()
    pool.pools = {"k1": cp1, "k2": cp2}

    _close_pool_manager(pool)

    cp1.close.assert_called_once_with()
    cp2.close.assert_called_once_with()
    pool.clear.assert_called_once_with()


def test_none_pool_is_a_noop():
    # A stop() racing ahead of pool assignment passes None; must not raise.
    _close_pool_manager(None)


def test_swallows_per_connection_close_errors_and_still_clears():
    bad = mock.Mock()
    bad.close.side_effect = RuntimeError("boom")
    good = mock.Mock()
    pool = mock.Mock()
    pool.pools = {"bad": bad, "good": good}

    _close_pool_manager(pool)  # must not propagate

    good.close.assert_called_once_with()
    pool.clear.assert_called_once_with()
