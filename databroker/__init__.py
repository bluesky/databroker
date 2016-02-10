import logging


logger = logging.getLogger(__name__)
__all__ = ['DataBroker', 'get_images', 'get_events', 'get_table', 'stream',
           'get_fields']


# generally useful imports
from .databroker import (DataBroker, DataBroker as db,
                         get_events, get_table, search, stream, get_fields)
from .pims_readers import get_images
from .handler_registration import register_builtin_handlers

# register all built-in filestore handlers
register_builtin_handlers()
del register_builtin_handlers

# set version string using versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
