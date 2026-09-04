"""Tests for the sync and async User-Agent tokens.

Sync and async ship in one package, so the User-Agent token is how LaunchDarkly
tells the two clients apart. These tests lock the exact tokens sent on the wire.
"""

from ldclient.config import Config
from ldclient.impl.http import ASYNC_USER_AGENT, SYNC_USER_AGENT, _base_headers
from ldclient.impl.util import _headers
from ldclient.version import VERSION


def _config():
    return Config(sdk_key='sdk-key')


def test_user_agent_tokens():
    assert SYNC_USER_AGENT == 'PythonClient'
    assert ASYNC_USER_AGENT == 'PythonAsyncClient'


def test_base_headers_default_user_agent_is_sync():
    assert _base_headers(_config())['User-Agent'] == 'PythonClient/' + VERSION


def test_base_headers_async_user_agent():
    assert _base_headers(_config(), ASYNC_USER_AGENT)['User-Agent'] == 'PythonAsyncClient/' + VERSION


def test_headers_default_user_agent_is_sync():
    assert _headers(_config())['User-Agent'] == 'PythonClient/' + VERSION


def test_headers_async_user_agent():
    assert _headers(_config(), ASYNC_USER_AGENT)['User-Agent'] == 'PythonAsyncClient/' + VERSION
