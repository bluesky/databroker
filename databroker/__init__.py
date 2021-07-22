import logging

from tiled.utils import OneShotCachedMap
from tiled.profiles import list_profiles
from tiled.client import from_profile

from ._factory_map import FactoryMap
from ._version import get_versions  # noqa: F402, E402

__version__ = get_versions()["version"]
del get_versions


logger = logging.getLogger(__name__)

# TODO This seems to load *all* items when one item is accessed.
catalog = FactoryMap(
    lambda: OneShotCachedMap(
        {
            # This `lambda profile=profile:` trick ensures that profile binds to
            # the each item in turn, rather than binding to the last item in
            # the loop. This is a "gotcha" in Python scoping.
            profile: lambda profile=profile: from_profile(profile)
            for profile in list_profiles()
        }
    )
)

# Everything below here is for backward-compatibility with v0 and v1 APIs.

# Support old top-level imports for backward compatibility,
# but do the imports lazily via module __getattr__ for speed.
_V1_IMPORTS = set("Broker Header ALL temp temp_config".split())
_UTILS_IMPORTS = set(
    (
        "lookup_config list_configs describe_configs "
        "wrap_in_doct DeprecatedDoct wrap_in_deprecated_doct "
        "catalog_search_path "
    ).split()
)
_DATABROKER_IMPORTS = set(
    (
        "DataBroker db get_events get_table stream get_fields restream process get_images"
    ).split()
)


def __getattr__(name):
    if name in _V1_IMPORTS:
        from . import v1

        return getattr(v1, name)
    if name in _UTILS_IMPORTS:
        from . import utils

        return getattr(utils, name)
    if name in _DATABROKER_IMPORTS:
        # This is a real mess...working around old poor choices.
        # Only very old (c. 2015) user code would hit this path.
        try:
            from .databroker import DataBroker

        except ImportError:
            # There is no legacy config defining a singleton Broker instance.
            # None of the objects in .databroker are defined.
            raise AttributeError(name)
        if name == "db":
            return DataBroker  # i.e. from .databroker import DataBroker as db
        if name == "get_images":
            # Likewise, this is only defined if there is a singleton
            # Broker instance.
            from .pims_readers import get_images

            return get_images
        return getattr(utils, name)
    raise AttributeError(name)


def __dir__():
    return sorted(
        list((set(globals()) | _V1_IMPORTS | _UTILS_IMPORTS | _DATABROKER_IMPORTS))
    )
