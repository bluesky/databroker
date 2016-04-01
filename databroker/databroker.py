"This module exists for back-compatability only."

from .broker  import Broker
from .core import get_fields  # unused, but here for API compat
from metadatastore.commands import _DB_SINGLETON as _MDS_SINGLETON
from filestore.api import _FS_SINGLETON

DataBroker = Broker(_MDS_SINGLETON, _FS_SINGLETON)

get_events = DataBroker.get_events
get_table = DataBroker.get_table
get_images = DataBroker.get_images
restream = DataBroker.restream
stream = DataBroker.stream
process = DataBroker.process
fill_event = DataBroker.fill_event
