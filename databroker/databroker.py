"This module exists for back-compatability only."

from .broker  import Broker
from .core import get_fields  # unused, but here for API compat
from metadatastore import conf as mds_conf, mds
from filestore import conf as fs_conf, fs

_mds_singleton = mds.MDSRO(mds_conf.connection_config)
_fs_singleton = fs.FileStoreRO(fs_conf.connection_config)
del mds
del mds_conf
del fs
del fs_conf

DataBroker = Broker(_mds_singleton, _fs_singleton)
get_events = DataBroker.get_events
get_table = DataBroker.get_table
get_images = DataBroker.get_images
restream = DataBroker.restream
stream = DataBroker.stream
process = DataBroker.process


def fill_event(*args, **kwargs):
    return DataBroker.fill_event(*args, **kwargs)

# flll_event = DataBroker.fill_event
