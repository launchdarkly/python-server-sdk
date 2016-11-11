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

__client = None
__config = Config()
__lock = ReadWriteLock()


# sets config.
def set_config(config):
    global __config
    global __client
    global __lock
    try:
        __lock.lock()
        if __client:
            log.info("Reinitializing LaunchDarkly Client " + version.VERSION + " with new config")
            new_client = LDClient(config, start_wait)
            old_client = __client
            __client = new_client
            old_client.close()
    finally:
        __config = config
        __lock.unlock()


# 2 use cases:
# 1. Initial setup: sets the sdk key for the uninitialized client
# 2. Allows on-the-fly changing of the sdk key. When this function is called after the client has been initialized
#    the client will get re-initialized with the new sdk key. In order for this to work, the return value of
#    ldclient.get() should never be assigned
def set_sdk_key(sdk_key):
    global __config
    global __client
    global __lock
    if sdk_key is __config.sdk_key:
        log.info("New sdk_key is the same as the existing one. doing nothing.")
    else:
        new_config = __config.copy_with_new_sdk_key(new_sdk_key=sdk_key)
        try:
            __lock.lock()
            if __client:
                log.info("Reinitializing LaunchDarkly Client " + version.VERSION + " with new sdk key")
                new_client = LDClient(new_config, start_wait)
                old_client = __client
                __config = new_config
                __client = new_client
                old_client.close()
        finally:
            __lock.unlock()


# the return value should not be assigned.
def get():
    global __config
    global __client
    global __lock
    try:
        __lock.rlock()
        if __client:
            return __client
    finally:
        __lock.runlock()

    try:
        global __client
        __lock.lock()
        if not __client:
            log.info("Initializing LaunchDarkly Client " + version.VERSION)
            __client = LDClient(__config, start_wait)
        return __client
    finally:
        __lock.unlock()


def init():
    return get()


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
