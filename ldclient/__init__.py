import threading

from .client import *
from ldclient.version import VERSION
from .util import log
import logging

__version__ = VERSION

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]


"""Settings."""
client = None
api_key = None
start_wait = 5
config = Config()

_lock = threading.Lock()


def get():
    try:
        _lock.acquire()
        global client
        if not client:
            log.debug("Initializing LaunchDarkly Client")
            client = LDClient(api_key, config, start_wait)
        return client
    finally:
        _lock.release()


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


try:
    from .twisted_impls import *
except ImportError:
    pass
