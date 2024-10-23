import os

import pytest

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


class SpyListener:
    def __init__(self):
        self._statuses = []

    def __call__(self, status):
        self._statuses.append(status)

    @property
    def statuses(self):
        return self._statuses
