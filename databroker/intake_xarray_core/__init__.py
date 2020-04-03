import intake  # Import this first to avoid circular imports during discovery.
from .xarray_container import RemoteXarray

import intake.container

intake.register_driver('databroker-remote-xarray', RemoteXarray)
intake.container.register_container('databroker-xarray', RemoteXarray)
