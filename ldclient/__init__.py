"""
The ldclient module contains the most common top-level entry points for the SDK.
"""

from ldclient.impl.rwlock import ReadWriteLock as _ReadWriteLock
from ldclient.impl.util import Result, log
from ldclient.version import VERSION

from .client import *
from .context import *
from .migrations import *

__version__ = VERSION

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email", "firstName", "lastName", "avatar", "name", "anonymous"]

"""Settings."""
start_wait = 5

__client = None
__config = None
__lock = _ReadWriteLock()


def set_config(config: Config):
    """Sets the configuration for the shared SDK client instance.

    If this is called prior to :func:`ldclient.get()`, it stores the configuration that will be used when the
    client is initialized. If it is called after the client has already been initialized, the client will be
    re-initialized with the new configuration (this will result in the next call to :func:`ldclient.get()`
    returning a new client instance).

    :param config: the client configuration
    """
    global __config
    global __client
    global __lock
    try:
        __lock.lock()
        if __client:
            log.info("Reinitializing LaunchDarkly Client " + VERSION + " with new config")
            new_client = LDClient(config=config, start_wait=start_wait)
            old_client = __client
            __client = new_client
            old_client.close()
    finally:
        __config = config
        __lock.unlock()


def get() -> LDClient:
    """Returns the shared SDK client instance, using the current global configuration.

    To use the SDK as a singleton, first make sure you have called :func:`ldclient.set_config()`
    at startup time. Then ``get()`` will return the same shared :class:`ldclient.client.LDClient`
    instance each time. The client will be initialized if it has not been already.

    If you need to create multiple client instances with different configurations, instead of this
    singleton approach you can call the :class:`ldclient.client.LDClient` constructor directly instead.
    """
    global __config
    global __client
    global __lock
    try:
        __lock.rlock()
        if __client:
            return __client
        if __config is None:
            raise Exception("set_config was not called")
    finally:
        __lock.runlock()

    try:
        __lock.lock()
        if not __client:
            log.info("Initializing LaunchDarkly Client " + VERSION)
            __client = LDClient(config=__config, start_wait=start_wait)
        return __client
    finally:
        __lock.unlock()


# for testing only
def _reset_client():
    global __client
    global __lock
    try:
        __lock.lock()
        c = __client
        __client = None
    finally:
        __lock.unlock()
    if c:
        c.close()


__BASE_TYPES__ = (str, float, int, bool)


__all__ = ['Config', 'Context', 'ContextBuilder', 'ContextMultiBuilder', 'LDClient', 'Result', 'client', 'context', 'evaluation', 'integrations', 'interfaces', 'migrations']
