# Import intake to run driver discovery first and avoid circular import issues.
import intake

import warnings
import logging


logger = logging.getLogger(__name__)


from .v1 import Broker, Header, ALL, temp, temp_config
from .utils import (lookup_config, list_configs, describe_configs,
                    wrap_in_doct, DeprecatedDoct, wrap_in_deprecated_doct,
                    catalog_search_path)

from .discovery import MergedCatalog, EntrypointsCatalog, V0Catalog

# A catalog created from discovered entrypoints, v0, and intake YAML catalogs.
from intake.catalog.default import load_combo_catalog
yaml_catalogs = load_combo_catalog()
catalog = MergedCatalog([
    EntrypointsCatalog(),
    V0Catalog(),
    load_combo_catalog()])

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
