from typing import Any, Dict, Optional

from ldclient.impl.util import log, redact_password
from ldclient.interfaces import AsyncBigSegmentStore, BigSegmentStoreMetadata

have_async_redis = False
try:
    import redis.asyncio as redis_client

    have_async_redis = True
except ImportError:
    pass


class _AsyncRedisBigSegmentStore(AsyncBigSegmentStore):
    KEY_LAST_UP_TO_DATE = ':big_segments_synchronized_on'
    KEY_USER_INCLUDE = ':big_segment_include:'
    KEY_USER_EXCLUDE = ':big_segment_exclude:'

    def __init__(self, url: str, prefix: Optional[str], redis_opts: Dict[str, Any]):
        if not have_async_redis:
            raise NotImplementedError("Cannot use async Redis Big Segment store because redis package is not installed")
        self._prefix = prefix or 'launchdarkly'
        self._client = redis_client.from_url(url, **redis_opts)
        log.info("Started AsyncRedisBigSegmentStore connected to URL: " + redact_password(url) + " using prefix: " + self._prefix)

    async def get_metadata(self) -> BigSegmentStoreMetadata:
        value = await self._client.get(self._prefix + self.KEY_LAST_UP_TO_DATE)
        return BigSegmentStoreMetadata(int(value) if value else None)

    async def get_membership(self, user_hash: str) -> Optional[dict]:
        included = await self._client.smembers(self._prefix + self.KEY_USER_INCLUDE + user_hash)
        excluded = await self._client.smembers(self._prefix + self.KEY_USER_EXCLUDE + user_hash)
        if not included and not excluded:
            return None
        ret = {ref.decode(): False for ref in excluded}
        ret.update({ref.decode(): True for ref in included})
        return ret

    async def stop(self):
        await self._client.aclose()
