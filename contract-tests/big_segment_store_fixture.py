import json
import os
import sys
from typing import Optional

import urllib3

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from ldclient.interfaces import BigSegmentStore, BigSegmentStoreMetadata

http = urllib3.PoolManager()


class BigSegmentStoreFixture(BigSegmentStore):
    def __init__(self, callback_uri: str):
        self._callback_uri = callback_uri

    def get_metadata(self) -> BigSegmentStoreMetadata:
        resp_data = self._post_callback('/getMetadata', None)
        return BigSegmentStoreMetadata(resp_data.get("lastUpToDate"))

    def get_membership(self, context_hash: str) -> Optional[dict]:
        resp_data = self._post_callback('/getMembership', {'contextHash': context_hash})
        return resp_data.get("values")

    def _post_callback(self, path: str, params: Optional[dict]) -> dict:
        url = self._callback_uri + path
        resp = http.request('POST', url, body=None if params is None else json.dumps(params), headers=None if params is None else {'Content-Type': 'application/json'})
        if resp.status != 200:
            raise Exception("HTTP error %d from callback to %s" % (resp.status, url))
        return json.loads(resp.data.decode('utf-8'))

    def stop(self):
        pass
