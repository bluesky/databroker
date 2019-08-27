from intake.catalog import Catalog


class Broker(Catalog):
    """
    This is a thin wrapper around intake.Catalog.

    It includes the option to return, in place of the usual intake Entries,
    v1-compatible Header objects.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._v1 = None

    @property
    def v1(self):
        "Accessor to the version 1 API."
        if self._v1 is None:
            from .v1 import Broker
            self._v1 = Broker(self)
        return self._v1

    @property
    def v2(self):
        return self
