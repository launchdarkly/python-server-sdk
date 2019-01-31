from ldclient.impl.integrations.files.file_data_source import _FileDataSource

class FileDataSource(UpdateProcessor):
    @classmethod
    def factory(cls, **kwargs):
        """Provides a way to use local files as a source of feature flag state. This would typically be
        used in a test environment, to operate using a predetermined feature flag state without an
        actual LaunchDarkly connection.

        This module and this implementation class are deprecated and may be changed or removed in the future.
        Please use :func:`ldclient.integrations.Files.new_data_source()`.
        
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

        return lambda config, store, ready : _FileDataSource(store, ready,
            paths=kwargs.get("paths"),
            auto_update=kwargs.get("auto_update", False),
            poll_interval=kwargs.get("poll_interval", 1),
            force_polling=kwargs.get("force_polling", False))
