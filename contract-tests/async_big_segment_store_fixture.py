from ldclient.interfaces import AsyncBigSegmentStore


class AsyncBigSegmentStoreFixture(AsyncBigSegmentStore):
    """AsyncBigSegmentStore implementation that calls back to the test harness."""

    def __init__(self, callback_uri: str):
        self._callback_uri = callback_uri

    async def get_metadata(self):
        from ldclient.interfaces import BigSegmentStoreMetadata
        resp_data = await self._post_callback('/getMetadata', None)
        return BigSegmentStoreMetadata(resp_data.get("lastUpToDate"))

    async def get_membership(self, context_hash: str):
        resp_data = await self._post_callback('/getMembership', {'contextHash': context_hash})
        return resp_data.get("values")

    async def _post_callback(self, path: str, params) -> dict:
        import aiohttp
        url = self._callback_uri + path
        async with aiohttp.ClientSession() as session:
            if params is None:
                async with session.post(url) as resp:
                    if resp.status != 200:
                        raise Exception("HTTP error %d from callback to %s" % (resp.status, url))
                    return await resp.json()
            else:
                async with session.post(url, json=params) as resp:
                    if resp.status != 200:
                        raise Exception("HTTP error %d from callback to %s" % (resp.status, url))
                    return await resp.json()

    async def stop(self):
        pass
