from ldclient.util import log
from ldclient.interfaces import FeatureStore
from ldclient.rwlock import ReadWriteLock


class InMemoryFeatureStore(FeatureStore):

    def __init__(self):
        self._lock = ReadWriteLock()
        self._initialized = False
        self._features = {}

    def get(self, key, callback):
        try:
            self._lock.rlock()
            f = self._features.get(key)
            if f is None:
                log.debug("Attempted to get missing feature: " + str(key) + " Returning None")
                return callback(None)
            if 'deleted' in f and f['deleted']:
                log.debug("Attempted to get deleted feature: " + str(key) + " Returning None")
                return callback(None)
            return callback(f)
        finally:
            self._lock.runlock()

    def all(self, callback):
        try:
            self._lock.rlock()
            return callback(dict((k, f) for k, f in self._features.items() if ('deleted' not in f) or not f['deleted']))
        finally:
            self._lock.runlock()

    def init(self, features):
        try:
            self._lock.lock()
            self._features = dict(features)
            self._initialized = True
            log.debug("Initialized feature store with " + str(len(features)) + " features")
        finally:
            self._lock.unlock()

    # noinspection PyShadowingNames
    def delete(self, key, version):
        try:
            self._lock.lock()
            f = self._features.get(key)
            if f is not None and f['version'] < version:
                f['deleted'] = True
                f['version'] = version
            elif f is None:
                f = {'deleted': True, 'version': version}
                self._features[key] = f
        finally:
            self._lock.unlock()

    def upsert(self, key, feature):
        try:
            self._lock.lock()
            f = self._features.get(key)
            if f is None or f['version'] < feature['version']:
                self._features[key] = feature
                log.debug("Updated feature {} to version {}".format(key, feature['version']))
        finally:
            self._lock.unlock()

    @property
    def initialized(self):
        try:
            self._lock.rlock()
            return self._initialized
        finally:
            self._lock.runlock()
