"This module exists for back-compatability only."
import warnings

from .broker import Broker
from .core import get_fields  # unused, but here for API compat

try:
    import metadatastore.conf
    mds_config = metadatastore.conf.load_configuration(
        'metadatastore', 'MDS', ['host', 'database', 'port', 'timezone'])

    import filestore.conf
    fs_config = filestore.conf.load_configuration('filestore', 'FS',
                                                  ['host', 'database', 'port'])

    from .assets.mongo import RegistryRO
    from .headersource.mongo import MDSRO
except (KeyError, ImportError) as exc:
    warnings.warn("No default DataBroker object will be created because "
                  "the necessary configuration was not found: %s" % exc)
else:
    DataBroker = Broker(MDSRO(mds_config), RegistryRO(fs_config))

    get_events = DataBroker.get_events
    get_table = DataBroker.get_table
    get_images = DataBroker.get_images
    restream = DataBroker.restream
    stream = DataBroker.stream
    process = DataBroker.process
    fill_event = DataBroker.fill_event
