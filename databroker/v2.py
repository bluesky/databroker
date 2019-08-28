from intake.catalog import Catalog


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
