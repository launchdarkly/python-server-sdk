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
start_wait = 5

_client = None
_config = Config()

__lock = ReadWriteLock()


def set_config(config):
    global _config
    _config = config


# 2 use cases:
# 1. Initial setup: sets the sdk key for the uninitialized client
# 2. Allows on-the-fly changing of the sdk key. When this function is called after the client has been initialized
#    the client will get re-initialized with the new sdk key. The calling code must then call ldclient.get() to use the
#    sdk key.
def set_sdk_key(sdk_key):
    global _config
    global _client
    global __lock
    if sdk_key is _config.sdk_key:
        log.info("New sdk_key is the same as the existing one. doing nothing.")
    else:
        new_config = _config.copy_with_new_sdk_key(new_sdk_key=sdk_key)
        try:
            __lock.lock()
            if _client:
                log.info("Re-initializing LaunchDarkly Client " + version.VERSION + " with new sdk key")
                new_client = LDClient(new_config, start_wait)
                print(new_client.get_sdk_key())
                old_client = _client
                _config = new_config
                print(_client.get_sdk_key())
                _client = new_client
                print(_client.get_sdk_key())
                old_client.close()
        finally:
            __lock.unlock()


def get():
    global _config
    global _client
    global __lock
    try:
        __lock.rlock()
        if _client:
            return _client
    finally:
        __lock.runlock()

    try:
        global _client
        __lock.lock()
        if not _client:
            log.info("Initializing LaunchDarkly Client " + version.VERSION)
            _client = LDClient(_config, start_wait)
        return _client
    finally:
        __lock.unlock()


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
