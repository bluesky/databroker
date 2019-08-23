# Import intake to run driver discovery first and avoid circular import issues.
import intake
del intake

import warnings
import logging


logger = logging.getLogger(__name__)


from ._core import (Broker, BrokerES, Header, ALL,
                    lookup_config, list_configs, describe_configs, temp_config,
                    wrap_in_doct,
                    DeprecatedDoct, wrap_in_deprecated_doct)

# set version string using versioneer
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


### Legacy imports ###

try:
    from .databroker import DataBroker
except ImportError:
    pass
else:
    from .databroker import (DataBroker, DataBroker as db,
                             get_events, get_table, stream, get_fields,
                             restream, process)
    from .pims_readers import get_images
