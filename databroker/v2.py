from intake.catalog import Catalog
from intake.catalog.entry import CatalogEntry
import entrypoints
import warnings

class Broker(Catalog):
    """
    This is a thin wrapper around intake.Catalog.

    It includes an accessor the databroker API version 1.
    """
    @property
    def v1(self):
        "Accessor to the version 1 API."
        if not hasattr(self, '_Broker__v1'):
            from .v1 import Broker
            self.__v1 = Broker(self)
        return self.__v1

    @property
    def v2(self):
        "A self-reference. This makes v1.Broker and v2.Broker symmetric."
        return self


class EntrypointEntry(CatalogEntry):
    """
    A catalog entry for an entrypoint.
    """
    def __init__(self, entrypoint):
        self._entrypoint = entrypoint
        self._name = entrypoint.name

    @property
    def name(self):
        return self._name

    def describe(self):
        """Basic information about this entry"""
        return {'name': self._name,
                'module_name': self._entrypoint.module_name,
                'object_name': self._entrypoint.object_name,
                'distro': self._entrypoint.distro,
                'extras': self._entrypoint.extras}

    def get(self):
        """Instantiate the DataSource for the given parameters"""
        return self._entrypoint.load()


class EntrypointsCatalog(Catalog):

    def __init__(self, *args, entrypoints_group='intake.catalogs', **kwargs):
        self._entrypoints_group = entrypoints_group
        super().__init__(*args, **kwargs)

    def _load(self):
        catalogs = entrypoints.get_group_named(self._entrypoints_group)
        self.name = self.name or 'EntrypointsCatalog'
        self.description = (self.description
                            or f'EntrypointsCatalog of {len(catalogs)} catalogs.')
        for name, entrypoint in catalogs.items():
            try:
                self._entries[name] = EntrypointEntry(entrypoint)
            except Exception as e:
                warings.warn(f"Failed to load {name}, {entrypoint}, {e!r}.")


class MergedCatalog(Catalog):
    def __init__(self, catalogs, *args, **kwargs):
        self._catalogs = catalogs
        super().__init__(*args, **kwargs)

    def _load(self):
        for catalog in self._catalogs:
            catalog._load()

    def _make_entries_container(self):
        return collections.ChainMap(*(catalog._entries for catalog in self._catalogs))


catalog = EntrypointsCatalog()
