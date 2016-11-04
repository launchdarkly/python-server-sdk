import logging

from ldclient.rwlock import ReadWriteLock
from ldclient.version import VERSION
from .client import *
from .util import log

__version__ = VERSION

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]


"""Settings."""
client = None
start_wait = 5
config = Config()

_lock = ReadWriteLock()


def get(sdk_key=None):
    try:
        _lock.rlock()
        if client:
            if sdk_key is not None:
                client.set_sdk_key(sdk_key)
            return client
    finally:
        _lock.runlock()

    try:
        global client
        _lock.lock()
        if not client:
            log.info("Initializing LaunchDarkly Client " + version.VERSION)
            config.sdk_key = sdk_key
            client = LDClient(config, start_wait)
        return client
    finally:
        _lock.unlock()


# Add a NullHandler for Python < 2.7 compatibility
class NullHandler(logging.Handler):

    def emit(self, record):
        pass


if not log.handlers:
    log.addHandler(NullHandler())

try:
    # noinspection PyUnresolvedReferences
    unicode
except NameError:
    __BASE_TYPES__ = (str, float, int, bool)
else:
    # noinspection PyUnresolvedReferences
    __BASE_TYPES__ = (str, float, int, bool, unicode)
