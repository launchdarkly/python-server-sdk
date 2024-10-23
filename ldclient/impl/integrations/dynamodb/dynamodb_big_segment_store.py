from typing import List, Optional

from ldclient.interfaces import BigSegmentStore, BigSegmentStoreMetadata

have_dynamodb = False
try:
    import boto3

    have_dynamodb = True
except ImportError:
    pass


class _DynamoDBBigSegmentStore(BigSegmentStore):
    PARTITION_KEY = 'namespace'
    SORT_KEY = 'key'
    KEY_METADATA = 'big_segments_metadata'
    KEY_USER_DATA = 'big_segments_user'
    ATTR_SYNC_TIME = 'synchronizedOn'
    ATTR_INCLUDED = 'included'
    ATTR_EXCLUDED = 'excluded'

    def __init__(self, table_name, prefix, dynamodb_opts):
        if not have_dynamodb:
            raise NotImplementedError("Cannot use DynamoDB Big Segment store because AWS SDK (boto3 package) is not installed")
        self._table_name = table_name
        self._prefix = (prefix + ":") if prefix else ""
        self._client = boto3.client('dynamodb', **dynamodb_opts)

    def get_metadata(self) -> BigSegmentStoreMetadata:
        key = self._prefix + self.KEY_METADATA
        data = self._client.get_item(TableName=self._table_name, Key={self.PARTITION_KEY: {"S": key}, self.SORT_KEY: {"S": key}})
        if data is not None:
            item = data.get('Item')
            if item is not None:
                attr = item.get(self.ATTR_SYNC_TIME)
                if attr is not None:
                    value = attr.get('N')
                    return BigSegmentStoreMetadata(None if value is None else int(value))
        return BigSegmentStoreMetadata(None)

    def get_membership(self, user_hash: str) -> Optional[dict]:
        data = self._client.get_item(TableName=self._table_name, Key={self.PARTITION_KEY: {"S": self._prefix + self.KEY_USER_DATA}, self.SORT_KEY: {"S": user_hash}})
        if data is not None:
            item = data.get('Item')
            if item is not None:
                included_refs = _get_string_list(item, self.ATTR_INCLUDED)
                excluded_refs = _get_string_list(item, self.ATTR_EXCLUDED)
                if (included_refs is None or len(included_refs) == 0) and (excluded_refs is None or len(excluded_refs) == 0):
                    return None
                ret = {}
                if excluded_refs is not None:
                    for seg_ref in excluded_refs:
                        ret[seg_ref] = False
                if included_refs is not None:
                    for seg_ref in included_refs:  # includes should override excludes
                        ret[seg_ref] = True
                return ret
        return None

    def stop(self):
        pass


def _get_string_list(item: dict, attr_name: str) -> Optional[List[str]]:
    attr = item.get(attr_name)
    if attr is None:
        return None
    return attr.get('SS')
