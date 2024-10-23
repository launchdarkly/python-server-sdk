from typing import Any, Dict, Optional, Set, cast

from ldclient import log
from ldclient.impl.util import redact_password
from ldclient.interfaces import BigSegmentStore, BigSegmentStoreMetadata

have_redis = False
try:
    import redis

    have_redis = True
except ImportError:
    pass


class _RedisBigSegmentStore(BigSegmentStore):
    KEY_LAST_UP_TO_DATE = ':big_segments_synchronized_on'
    KEY_USER_INCLUDE = ':big_segment_include:'
    KEY_USER_EXCLUDE = ':big_segment_exclude:'

    def __init__(self, url: str, prefix: Optional[str], redis_opts: Dict[str, Any]):
        if not have_redis:
            raise NotImplementedError("Cannot use Redis Big Segment store because redis package is not installed")
        self._prefix = prefix or 'launchdarkly'
        self._pool = redis.ConnectionPool.from_url(url=url, **redis_opts)
        log.info("Started RedisBigSegmentStore connected to URL: " + redact_password(url) + " using prefix: " + self._prefix)

    def get_metadata(self) -> BigSegmentStoreMetadata:
        r = redis.Redis(connection_pool=self._pool)
        value = r.get(self._prefix + self.KEY_LAST_UP_TO_DATE)
        if value is None:
            return BigSegmentStoreMetadata(None)

        return BigSegmentStoreMetadata(int(value))

    def get_membership(self, user_hash: str) -> Optional[dict]:
        r = redis.Redis(connection_pool=self._pool)
        included_refs = cast(Set[bytes], r.smembers(self._prefix + self.KEY_USER_INCLUDE + user_hash))
        excluded_refs = cast(Set[bytes], r.smembers(self._prefix + self.KEY_USER_EXCLUDE + user_hash))
        # The cast to Set[bytes] is because the linter is otherwise confused about the return type of smembers
        # and thinks there could be some element type other than bytes.
        if (included_refs is None or len(included_refs) == 0) and (excluded_refs is None or len(excluded_refs) == 0):
            return None
        ret = {}
        for seg_ref in excluded_refs:
            ret[seg_ref.decode()] = False
        for seg_ref in included_refs:  # includes should override excludes
            ret[seg_ref.decode()] = True
        return ret

    def stop(self):
        self._pool.disconnect()
