import os

skip_database_tests = os.environ.get('LD_SKIP_DATABASE_TESTS') == '1'


class SpyListener:
    def __init__(self):
        self._statuses = []

    def __call__(self, status):
        self._statuses.append(status)

    @property
    def statuses(self):
        return self._statuses
