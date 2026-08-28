"""
This module holds the shared, I/O-free helpers for big-segments status
tracking. The helpers (`_hash_for_user_key`, `is_stale`, `EMPTY_MEMBERSHIP`)
are shared by both the sync :mod:`ldclient.impl.big_segments` and the async
:mod:`ldclient.impl.async_big_segments` managers; nothing here touches the
store or network.
"""

import base64
import time
from hashlib import sha256

# use EMPTY_MEMBERSHIP as a singleton whenever a membership query returns None; it's safe to reuse it
# because we will never modify the membership properties after they're queried
EMPTY_MEMBERSHIP = {}  # type: dict


def _hash_for_user_key(user_key: str) -> str:
    return base64.b64encode(sha256(user_key.encode('utf-8')).digest()).decode('utf-8')


def is_stale(timestamp: int, stale_after_millis) -> bool:
    return (timestamp is None) or ((int(time.time() * 1000) - timestamp) >= stale_after_millis)
