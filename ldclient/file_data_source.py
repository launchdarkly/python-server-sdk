import json
import six
import traceback

have_yaml = False
try:
    import yaml
    have_yaml = True
except ImportError:
    pass

from ldclient.interfaces import UpdateProcessor
from ldclient.util import log
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class FileDataSource(UpdateProcessor):
    @classmethod
    def factory(cls, **kwargs):
        return lambda config, store, ready : FileDataSource(store, kwargs, ready)
    
    def __init__(self, store, options, ready):
        self._store = store
        self._ready = ready
        self._inited = False
        self._paths = options.get('paths', [])
        if isinstance(self._paths, six.string_types):
            self._paths = [ self._paths ]

    def start(self):
        self._load_all()

        # We will signal readiness immediately regardless of whether the file load succeeded or failed -
        # the difference can be detected by checking initialized()
        self._ready.set() 

    def stop(self):
        pass

    def initialized(self):
        return self._inited

    def _load_all(self):
        all_data = { FEATURES: {}, SEGMENTS: {} }
        for path in self._paths:
            try:
                self._load_file(path, all_data)
            except Exception as e:
                log.error('Unable to load flag data from "%s": %s' % (path, repr(e)))
                traceback.print_exc()
                return
        print "Initing: %s" % all_data
        self._store.init(all_data)
        self._inited = True
    
    def _load_file(self, path, all_data):
        content = None
        with open(path, 'r') as f:
            content = f.read()
        parsed = self._parse_content(content)
        for key, flag in six.iteritems(parsed.get('flags', {})):
            self._add_item(all_data, FEATURES, flag)
        for key, value in six.iteritems(parsed.get('flagValues', {})):
            self._add_item(all_data, FEATURES, self._make_flag_with_value(key, value))
        for key, segment in six.iteritems(parsed.get('segments', {})):
            self._add_item(all_data, SEGMENTS, segment)
    
    def _parse_content(self, content):
        if have_yaml:
            if content.strip().startswith("{"):
                print("json: %s" % content)
                return json.loads(content)
            else:
                return yaml.load(content)
        print("json: %s" % content)
        return json.loads(content)
    
    def _add_item(self, all_data, kind, item):
        items = all_data[kind]
        key = item.get('key')
        if items.get(key) is None:
            items[key] = item
        else:
            raise Exception('In %s, key "%s" was used more than once' % (kind.namespace, key))

    def _make_flag_with_value(self, key, value):
        return {
            'key': key,
            'on': True,
            'fallthrough': {
                'variation': 0
            },
            'variations': [ value ]
        }
