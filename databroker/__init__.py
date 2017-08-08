import warnings
import logging


logger = logging.getLogger(__name__)


try:
    import attr  # confusingly, this comes from 'attrs' on PyPI, not 'attr'
    attr.s
except AttributeError:
    raise ImportError("Wrong attr module imported. Please uninstall 'attr' "
                      "and install 'attrs' which provides the correct module.")


try:
    from .databroker import DataBroker
except ImportError:
    # The .databroker module emits a warning, no need to duplicate it here.
    pass
else:
    from .databroker import (DataBroker, DataBroker as db,
                             get_events, get_table, stream, get_fields,
                             restream, process)
    from .pims_readers import get_images


from .broker import Broker, ArchiverEventSource

# set version string using versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
