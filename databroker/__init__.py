import warnings
import logging


logger = logging.getLogger(__name__)


try:
    from .databroker import DataBroker
except ImportError:
    warnings.warn("The top-level functions (get_table, get_events, etc.)"
                  "cannot be created because "
                  "the necessary configuration was not found.")
else:
    from .databroker import (DataBroker, DataBroker as db,
                             get_events, get_table, stream, get_fields,
                             restream, process)
    from .pims_readers import get_images
    from .handler_registration import register_builtin_handlers

    # register all built-in filestore handlers
    register_builtin_handlers()
    del register_builtin_handlers

from .broker import Broker, ArchiverPlugin

# set version string using versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
