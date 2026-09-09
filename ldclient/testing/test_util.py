import os
from contextlib import contextmanager
from unittest import mock

import pytest

from ldclient.impl import retry
from ldclient.impl.util import redact_password

skip_database_tests = os.environ.get('LD_SKIP_DATABASE_TESTS') == '1'


@pytest.fixture(
    params=[
        ("rediss://user:password=@redis-server-url:6380/0?ssl_cert_reqs=CERT_REQUIRED", "rediss://user:xxxx@redis-server-url:6380/0?ssl_cert_reqs=CERT_REQUIRED"),
        ("rediss://user-matches-password:user-matches-password@redis-server-url:6380/0?ssl_cert_reqs=CERT_REQUIRED", "rediss://xxxx:xxxx@redis-server-url:6380/0?ssl_cert_reqs=CERT_REQUIRED"),
        ("rediss://redis-server-url", "rediss://redis-server-url"),
        ("invalid urls are left alone", "invalid urls are left alone"),
    ]
)
def password_redaction_tests(request):
    return request.param


def test_can_redact_password(password_redaction_tests):
    input, expected = password_redaction_tests

    assert redact_password(input) == expected


class _FixedRandom:
    """Stands in for the ``random`` module, always drawing the same value."""

    def __init__(self, value: float):
        self.value = value

    def random(self) -> float:
        return self.value


@contextmanager
def fixed_retry_jitter(fraction: float):
    """Fixes the jitter that :mod:`ldclient.impl.retry` subtracts from a delay.

    ``0`` subtracts none, so a test can assert an exact delay. A value just
    below ``1`` subtracts as much as the spec allows, which is half.

    Patching the retry module's own ``random`` reference keeps the change
    local to that module; every other module keeps the real source.
    """
    with mock.patch.object(retry, 'random', _FixedRandom(fraction)):
        yield


def no_retry_jitter():
    """Removes the retry jitter, so a test can assert an exact delay."""
    return fixed_retry_jitter(0.0)


class SpyListener:
    def __init__(self):
        self._statuses = []

    def __call__(self, status):
        self._statuses.append(status)

    @property
    def statuses(self):
        return self._statuses
