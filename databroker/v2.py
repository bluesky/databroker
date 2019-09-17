from pathlib import Path
import tempfile

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


def temp():
    """
    Generate a Catalog backed by a temporary directory of msgpack-encoded files.
    """
    from databroker._drivers.msgpack import BlueskyMsgpackCatalog
    handler_registry = {}
    # Let ophyd be an optional dependency.
    # If it is not installed, then we clearly do not need its handler for this
    # temporary data store.
    try:
        import ophyd.sim
    except ImportError:
        pass
    else:
        handler_registry['NPY_SEQ'] = ophyd.sim.NumpySeqHandler
    tmp_dir = tempfile.mkdtemp()
    tmp_data_dir = Path(tmp_dir) / 'data'
    catalog = BlueskyMsgpackCatalog(
        f"{tmp_data_dir}/*.msgpack",
        name='temp',
        handler_registry=handler_registry)
    return catalog
