class Broker:
    """
    This supports the original Broker API but implemented on intake.Catalog.
    """
    def __init__(self, uri, source, header_version=2):
        catalog = Catalog(str(uri))
        if source is not None:
            catalog = catalog[source]()
        self._catalog = catalog
        self.header_version = header_version
        self.external_fetchers = external_fetchers or {}

        from .v1 import Broker
        self._v1 = Broker(uri, source)

    @property
    def v1(self):
        "Accessor to the version 1 API."
        return self._v1
