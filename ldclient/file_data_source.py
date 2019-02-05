"""
Deprecated entry point for a component that has been moved.
"""
# currently excluded from documentation - see docs/README.md

from ldclient.impl.integrations.files.file_data_source import _FileDataSource
from ldclient.interfaces import UpdateProcessor

class FileDataSource(UpdateProcessor):
    @classmethod
    def factory(cls, **kwargs):
        """Provides a way to use local files as a source of feature flag state.
        
        .. deprecated:: 6.8.0
          This module and this implementation class are deprecated and may be changed or removed in the future.
          Please use :func:`ldclient.integrations.Files.new_data_source()`.
        
        The keyword arguments are the same as the arguments to :func:`ldclient.integrations.Files.new_data_source()`.
        """

        return lambda config, store, ready : _FileDataSource(store, ready,
            paths=kwargs.get("paths"),
            auto_update=kwargs.get("auto_update", False),
            poll_interval=kwargs.get("poll_interval", 1),
            force_polling=kwargs.get("force_polling", False))
