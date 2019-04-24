"""
The ldclient module contains the most common top-level entry points for the SDK.
"""

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


def set_config(config):
    """Sets the configuration for the shared SDK client instance.

    If this is called prior to :func:`ldclient.get()`, it stores the configuration that will be used when the
    client is initialized. If it is called after the client has already been initialized, the client will be
    re-initialized with the new configuration (this will result in the next call to :func:`ldclient.get()`
    returning a new client instance).

    :param ldclient.config.Config config: the client configuration
    """
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


def set_sdk_key(sdk_key):
    """Sets the SDK key for the shared SDK client instance.

    If this is called prior to :func:`ldclient.get()`, it stores the SDK key that will be used when the client is
    initialized. If it is called after the client has already been initialized, the client will be
    re-initialized with the new SDK key (this will result in the next call to :func:`ldclient.get()` returning a
    new client instance).

    If you need to set any configuration options other than the SDK key, use :func:`ldclient.set_config()` instead.

    :param string sdk_key: the new SDK key
    """
    global __config
    global __client
    global __lock
    sdk_key_changed = False
    try:
        __lock.rlock()
        if sdk_key == __config.sdk_key:
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
    """Returns the shared SDK client instance, using the current global configuration.

    To use the SDK as a singleton, first make sure you have called :func:`ldclient.set_sdk_key()` or
    :func:`ldclient.set_config()` at startup time. Then ``get()`` will return the same shared
    :class:`ldclient.client.LDClient` instance each time. The client will be initialized if it has
    not been already.

    If you need to create multiple client instances with different configurations, instead of this
    singleton approach you can call the :class:`ldclient.client.LDClient` constructor directly instead.

    :rtype: ldclient.client.LDClient
    """
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


# currently hidden from documentation - see docs/README.md
class NullHandler(logging.Handler):
    """A :class:`logging.Handler` implementation that does nothing.

    .. deprecated:: 6.0.0
      You should not need to use this class. It was originally used in order to support Python 2.6,
      which requires that at least one logging handler must always be configured. However, the SDK
      no longer supports Python 2.6.
    """
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
