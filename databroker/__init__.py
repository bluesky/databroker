# Import intake to run driver discovery first and avoid circular import issues.
import logging

import intake  # noqa: F401
from intake.catalog.default import load_combo_catalog

from .v1 import Broker, Header, ALL, temp, temp_config  # noqa: F401
from .utils import (  # noqa: 401
    lookup_config, list_configs, describe_configs,  # noqa: F401
    wrap_in_doct, DeprecatedDoct, wrap_in_deprecated_doct,  # noqa: F401
    catalog_search_path)  # noqa: F401

from .discovery import MergedCatalog, EntrypointsCatalog, V0Catalog


logger = logging.getLogger(__name__)


# A catalog created from discovered entrypoints, v0, and intake YAML catalogs.
yaml_catalogs = load_combo_catalog()
catalog = MergedCatalog([
    EntrypointsCatalog(),
    V0Catalog(),
    load_combo_catalog()])

# set version string using versioneer
from ._version import get_versions  # noqa: F402, E402
__version__ = get_versions()['version']
del get_versions


# Legacy imports

try:
    from .databroker import DataBroker
except ImportError:
    pass
else:
    from .databroker import (DataBroker,  # noqa: 811
                             DataBroker as db,
                             get_events, get_table, stream, get_fields,
                             restream, process)
    from .pims_readers import get_images  # noqa: F401
