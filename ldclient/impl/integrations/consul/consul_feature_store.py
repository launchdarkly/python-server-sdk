import json

have_consul = False
try:
    import consul
    have_consul = True
except ImportError:
    pass

from ldclient import log
from ldclient.feature_store import CacheConfig
from ldclient.feature_store_helpers import CachingStoreWrapper
from ldclient.interfaces import DiagnosticDescription, FeatureStore, FeatureStoreCore

# 
# Internal implementation of the Consul feature store.
# 
# Implementation notes:
# 
# * Feature flags, segments, and any other kind of entity the LaunchDarkly client may wish
# to store, are stored as individual items with the key "{prefix}/features/{flag-key}",
# "{prefix}/segments/{segment-key}", etc.
#
# * The special key "{prefix}/$inited" indicates that the store contains a complete data set.
#
# * Since Consul has limited support for transactions (they can't contain more than 64
# operations), the init method-- which replaces the entire data store-- is not guaranteed to
# be atomic, so there can be a race condition if another process is adding new data via
# Upsert. To minimize this, we don't delete all the data at the start; instead, we update
# the items we've received, and then delete all other items. That could potentially result in
# deleting new data from another process, but that would be the case anyway if the Init
# happened to execute later than the Upsert; we are relying on the fact that normally the
# process that did the Init will also receive the new data shortly and do its own Upsert.
# 

class _ConsulFeatureStoreCore(DiagnosticDescription, FeatureStoreCore):
    def __init__(self, host, port, prefix, consul_opts):
        if not have_consul:
            raise NotImplementedError("Cannot use Consul feature store because the python-consul package is not installed")
        opts = consul_opts or {}
        if host is not None:
            opts['host'] = host
        if port is not None:
            opts['port'] = port
        self._prefix = ("launchdarkly" if prefix is None else prefix) + "/"
        self._client = consul.Consul(**opts)

    def init_internal(self, all_data):
        # Start by reading the existing keys; we will later delete any of these that weren't in all_data.
        index, keys = self._client.kv.get(self._prefix, recurse=True, keys=True)
        unused_old_keys = set(keys or [])

        num_items = 0
        inited_key = self._inited_key()
        unused_old_keys.discard(inited_key)

        # Insert or update every provided item. Note that this Consul client doesn't support batch
        # operations (the "txn" method), so we'll write them one at a time.
        for kind, items in all_data.items():
            for key, item in items.items():
                encoded_item = json.dumps(item)
                db_key = self._item_key(kind, item['key'])
                self._client.kv.put(db_key, encoded_item)
                unused_old_keys.discard(db_key)
                num_items = num_items + 1

        # Now delete any previously existing items whose keys were not in the current data
        for key in unused_old_keys:
            self._client.kv.delete(key)
        
        # Now set the special key that we check in initialized_internal()
        self._client.kv.put(inited_key, "")

        log.info('Initialized Consul store with %d items', num_items)

    def get_internal(self, kind, key):
        index, resp = self._client.kv.get(self._item_key(kind, key))
        return None if resp is None else json.loads(resp['Value'].decode('utf-8'))

    def get_all_internal(self, kind):
        items_out = {}
        index, results = self._client.kv.get(self._kind_key(kind), recurse=True)
        for result in results:
            item = json.loads(result['Value'].decode('utf-8'))
            items_out[item['key']] = item
        return items_out

    def upsert_internal(self, kind, new_item):
        key = self._item_key(kind, new_item['key'])
        encoded_item = json.dumps(new_item)

        # We will potentially keep retrying indefinitely until someone's write succeeds
        while True:
            index, old_value = self._client.kv.get(key)
            if old_value is None:
                mod_index = 0
            else:
                old_item = json.loads(old_value['Value'].decode('utf-8'))
                # Check whether the item is stale. If so, don't do the update (and return the existing item to
                # CachingStoreWrapper so it can be cached)
                if old_item['version'] >= new_item['version']:
                    return old_item
                mod_index = old_value['ModifyIndex']

            # Otherwise, try to write. We will do a compare-and-set operation, so the write will only succeed if
            # the key's ModifyIndex is still equal to the previous value. If the previous ModifyIndex was zero,
            # it means the key did not previously exist and the write will only succeed if it still doesn't exist.
            success = self._client.kv.put(key, encoded_item, cas=mod_index)
            if success:
                return new_item

            log.debug('Concurrent modification detected, retrying')

    def initialized_internal(self):
        index, resp = self._client.kv.get(self._inited_key())
        return (resp is not None)

    def describe_configuration(self, config):
        return 'Consul'
    
    def _kind_key(self, kind):
        return self._prefix + kind.namespace

    def _item_key(self, kind, key):
        return self._kind_key(kind) + '/' + key

    def _inited_key(self):
        return self._prefix + ('$inited')
