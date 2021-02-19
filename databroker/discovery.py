from databroker.utils import list_configs, lookup_config, CONFIG_SEARCH_PATH
from intake.catalog import Catalog
from intake.catalog.entry import CatalogEntry

# Import from intake, so that code that relies on
# databroker.discovery.MergedCatalog, etc. still works.
from intake.catalog.local import MergedCatalog, EntrypointsCatalog  # noqa: F401
from intake.catalog.local import EntrypointEntry  # noqa: F401


class V0Entry(CatalogEntry):
    # Work around
    # https://github.com/intake/intake/issues/545
    _container = None

    def __init__(self, name, *args, **kwargs):
        self._name = name
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return f"<Entry containing Catalog named {self._name}>"

    def describe(self):
        return {"name": self._name}

    def get(self):
        # Hide this import here so that
        # importing v2 doesn't import v1 unless we actually use it.
        from databroker import v1
        config = lookup_config(self._name)
        catalog = v1.from_config(config)  # might return v0, v1, or v2 Broker
        if not hasattr(catalog, 'v2'):
            raise ValueError("The config file could not be parsed for v2-style access.")
        return catalog.v2  # works if catalog is v1-style or v2-style


class V0Catalog(Catalog):
    """
    Build v2.Brokers based on any v0-style configs we can find.
    """
    # Work around
    # https://github.com/intake/intake/issues/545
    _container = None

    def __init__(self, *args, paths=CONFIG_SEARCH_PATH, **kwargs):
        self._paths = paths
        super().__init__(*args, **kwargs)

    def _load(self):
        for name in list_configs(paths=self._paths):
            self._entries[name] = V0Entry(name)
