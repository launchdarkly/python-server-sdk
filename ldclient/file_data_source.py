import json
import os
import six
import traceback

have_yaml = False
try:
    import yaml
    have_yaml = True
except ImportError:
    pass

have_watchdog = False
try:
    import watchdog
    import watchdog.events
    import watchdog.observers
    have_watchdog = True
except ImportError:
    pass

from ldclient.interfaces import UpdateProcessor
from ldclient.repeating_timer import RepeatingTimer
from ldclient.util import log
from ldclient.versioned_data_kind import FEATURES, SEGMENTS


class FileDataSource(UpdateProcessor):
    @classmethod
    def factory(cls, **kwargs):
        """Provides a way to use local files as a source of feature flag state. This would typically be
        used in a test environment, to operate using a predetermined feature flag state without an
        actual LaunchDarkly connection.

        To use this component, call `FileDataSource.factory`, and store its return value in the
        `update_processor_class` property of your LaunchDarkly client configuration. In the options
        to `factory`, set `paths` to the file path(s) of your data file(s):
        ::

            factory = FileDataSource.factory(paths = [ myFilePath ])
            config = Config(update_processor_class = factory)

        This will cause the client not to connect to LaunchDarkly to get feature flags. The
        client may still make network connections to send analytics events, unless you have disabled
        this with Config.send_events or Config.offline.

        Flag data files can be either JSON or YAML (in order to use YAML, you must install the 'pyyaml'
        package). They contain an object with three possible properties:

        * "flags": Feature flag definitions.
        * "flagValues": Simplified feature flags that contain only a value.
        * "segments": User segment definitions.

        The format of the data in "flags" and "segments" is defined by the LaunchDarkly application
        and is subject to change. Rather than trying to construct these objects yourself, it is simpler
        to request existing flags directly from the LaunchDarkly server in JSON format, and use this
        output as the starting point for your file. In Linux you would do this:
        ::

            curl -H "Authorization: {your sdk key}" https://app.launchdarkly.com/sdk/latest-all

        The output will look something like this (but with many more properties):
        ::

            {
                "flags": {
                    "flag-key-1": {
                    "key": "flag-key-1",
                    "on": true,
                    "variations": [ "a", "b" ]
                    }
                },
                "segments": {
                    "segment-key-1": {
                    "key": "segment-key-1",
                    "includes": [ "user-key-1" ]
                    }
                }
            }

        Data in this format allows the SDK to exactly duplicate all the kinds of flag behavior supported
        by LaunchDarkly. However, in many cases you will not need this complexity, but will just want to
        set specific flag keys to specific values. For that, you can use a much simpler format:
        ::

            {
                "flagValues": {
                    "my-string-flag-key": "value-1",
                    "my-boolean-flag-key": true,
                    "my-integer-flag-key": 3
                }
            }

        Or, in YAML:
        ::

            flagValues:
            my-string-flag-key: "value-1"
            my-boolean-flag-key: true
            my-integer-flag-key: 1

        It is also possible to specify both "flags" and "flagValues", if you want some flags
        to have simple values and others to have complex behavior. However, it is an error to use the
        same flag key or segment key more than once, either in a single file or across multiple files.

        If the data source encounters any error in any file-- malformed content, a missing file, or a
        duplicate key-- it will not load flags from any of the files.      

        :param kwargs:
            See below

        :Keyword arguments:
        * **paths** (array): The paths of the source files for loading flag data. These may be absolute paths
          or relative to the current working directory. Files will be parsed as JSON unless the 'pyyaml'
          package is installed, in which case YAML is also allowed.
        * **auto_update** (boolean): True if the data source should watch for changes to the source file(s)
          and reload flags whenever there is a change. The default implementation of this feature is based on
          polling the filesystem, which may not perform well; if you install the 'watchdog' package (not
          included by default, to avoid adding unwanted dependencies to the SDK), its native file watching
          mechanism will be used instead. Note that auto-updating will only work if all of the files you
          specified have valid directory paths at startup time.
        * **poll_interval** (float): The minimum interval, in seconds, between checks for file modifications -
          used only if auto_update is true, and if the native file-watching mechanism from 'watchdog' is not
          being used. The default value is 1 second.
        """
        return lambda config, store, ready : FileDataSource(store, kwargs, ready)
    
    def __init__(self, store, options, ready):
        self._store = store
        self._ready = ready
        self._inited = False
        self._paths = options.get('paths', [])
        if isinstance(self._paths, six.string_types):
            self._paths = [ self._paths ]
        self._auto_update = options.get('auto_update', False)
        self._auto_updater = None
        self._poll_interval = options.get('poll_interval', 1)
        self._force_polling = options.get('force_polling', False)  # used only in tests
        
    def start(self):
        self._load_all()

        if self._auto_update:
            self._auto_updater = self._start_auto_updater()
        
        # We will signal readiness immediately regardless of whether the file load succeeded or failed -
        # the difference can be detected by checking initialized()
        self._ready.set() 

    def stop(self):
        if self._auto_updater:
            self._auto_updater.stop()

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
            return yaml.load(content)  # pyyaml correctly parses JSON too
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

    def _start_auto_updater(self):
        resolved_paths = []
        for path in self._paths:
            try:
                resolved_paths.append(os.path.realpath(path))
            except:
                log.warn('Cannot watch for changes to data file "%s" because it is an invalid path' % path)
        if have_watchdog and not self._force_polling:
            return FileDataSource.WatchdogAutoUpdater(resolved_paths, self._load_all)
        else:
            return FileDataSource.PollingAutoUpdater(resolved_paths, self._load_all, self._poll_interval)
    
    # Watch for changes to data files using the watchdog package. This uses native OS filesystem notifications
    # if available for the current platform.
    class WatchdogAutoUpdater(object):
        def __init__(self, resolved_paths, reloader):
            watched_files = set(resolved_paths)

            class LDWatchdogHandler(watchdog.events.FileSystemEventHandler):
                def on_any_event(self, event):
                    if event.src_path in watched_files:
                        reloader()
            
            dir_paths = set()
            for path in resolved_paths:
                dir_paths.add(os.path.dirname(path))
            
            self._observer = watchdog.observers.Observer()
            handler = LDWatchdogHandler()
            for path in dir_paths:
                self._observer.schedule(handler, path)
            self._observer.start()

        def stop(self):
            self._observer.stop()
            self._observer.join()

    # Watch for changes to data files by polling their modification times. This is used if auto-update is
    # on but the watchdog package is not installed.
    class PollingAutoUpdater(object):
        def __init__(self, resolved_paths, reloader, interval):
            self._paths = resolved_paths
            self._reloader = reloader
            self._file_times = self._check_file_times()
            self._timer = RepeatingTimer(interval, self._poll)
            self._timer.start()
        
        def stop(self):
            self._timer.stop()
        
        def _poll(self):
            new_times = self._check_file_times()
            changed = False
            for file_path, file_time in six.iteritems(self._file_times):
                if new_times.get(file_path) is not None and new_times.get(file_path) != file_time:
                    changed = True
                    break
            self._file_times = new_times
            if changed:
                self._reloader()

        def _check_file_times(self):
            ret = {}
            for path in self._paths:
                try:
                    ret[path] = os.path.getmtime(path)
                except:
                    ret[path] = None
            return ret
