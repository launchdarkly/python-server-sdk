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


# 2 Use Cases:
# 1. Initial setup: sets the config for the uninitialized client
# 2. Allows on-the-fly changing of the config. When this function is called after the client has been initialized
#    the client will get re-initialized with the new config. In order for this to work, the return value of
#    ldclient.get() should never be assigned
def set_config(config):
    global __config
    global __client
    global __lock
    try:
        __lock.lock()
        if __client:
            log.info("Reinitializing LaunchDarkly Client " + version.VERSION + " with new config")
            new_client = LDClient(config=config, start_wait=start_wait)
            old_client = __client
            __client = new_client
            old_client.close()
    finally:
        __config = config
        __lock.unlock()


# 2 Use Cases:
# 1. Initial setup: sets the sdk key for the uninitialized client
# 2. Allows on-the-fly changing of the sdk key. When this function is called after the client has been initialized
#    the client will get re-initialized with the new sdk key. In order for this to work, the return value of
#    ldclient.get() should never be assigned
def set_sdk_key(sdk_key):
    global __config
    global __client
    global __lock
    sdk_key_changed = False
    try:
        __lock.rlock()
        if sdk_key is __config.sdk_key:
            log.info("New sdk_key is the same as the existing one. doing nothing.")
        else:
            sdk_key_changed = True
    finally:
        __lock.runlock()

    if sdk_key_changed:
        try:
            __lock.lock()
            __config = __config.copy_with_new_sdk_key(new_sdk_key=sdk_key)
            if __client:
                log.info("Reinitializing LaunchDarkly Client " + version.VERSION + " with new sdk key")
                new_client = LDClient(config=__config, start_wait=start_wait)
                old_client = __client
                __client = new_client
                old_client.close()
        finally:
            __lock.unlock()


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
        __lock.lock()
        if not __client:
            log.info("Initializing LaunchDarkly Client " + version.VERSION)
            __client = LDClient(config=__config, start_wait=start_wait)
        return __client
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
