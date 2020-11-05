import collections
from databroker.utils import list_configs, lookup_config, CONFIG_SEARCH_PATH
import entrypoints
from intake.catalog import Catalog
from intake.catalog.entry import CatalogEntry
import warnings


class EntrypointEntry(CatalogEntry):
    """
    A catalog entry for an entrypoint.
    """
    def __init__(self, entrypoint):
        self._entrypoint = entrypoint

    def __repr__(self):
        return f"<Entry containing Catalog named {self.name}>"

    @property
    def name(self):
        return self._entrypoint.name

    def describe(self):
        """Basic information about this entry"""
        return {'name': self.name,
                'module_name': self._entrypoint.module_name,
                'object_name': self._entrypoint.object_name,
                'distro': self._entrypoint.distro,
                'extras': self._entrypoint.extras}

    def get(self):
        """Instantiate the DataSource for the given parameters"""
        return self._entrypoint.load()


class EntrypointsCatalog(Catalog):
    """
    A catalog of discovered entrypoint catalogs.
    """

    def __init__(self, *args, entrypoints_group='intake.catalogs', paths=None,
                 **kwargs):
        self._entrypoints_group = entrypoints_group
        self._paths = paths
        super().__init__(*args, **kwargs)

    def _load(self):
        catalogs = entrypoints.get_group_named(self._entrypoints_group,
                                               path=self._paths)
        self.name = self.name or 'EntrypointsCatalog'
        self.description = (self.description
                            or f'EntrypointsCatalog of {len(catalogs)} catalogs.')
        for name, entrypoint in catalogs.items():
            try:
                self._entries[name] = EntrypointEntry(entrypoint)
            except Exception as e:
                warnings.warn(f"Failed to load {name}, {entrypoint}, {e!r}.")


class V0Entry(CatalogEntry):

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
    def __init__(self, *args, paths=CONFIG_SEARCH_PATH, **kwargs):
        self._paths = paths
        super().__init__(*args, **kwargs)

    def _load(self):
        for name in list_configs(paths=self._paths):
            self._entries[name] = V0Entry(name)


class MergedCatalog(Catalog):
    """
    A Catalog that merges the entries of a list of catalogs.
    """
    def __init__(self, catalogs, *args, **kwargs):
        self._catalogs = catalogs
        super().__init__(*args, **kwargs)

    def _load(self):
        for catalog in self._catalogs:
            catalog._load()

    def _make_entries_container(self):
        return collections.ChainMap(*(catalog._entries for catalog in self._catalogs))
