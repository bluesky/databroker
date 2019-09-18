import intake  # Import this first to avoid circular imports during discovery.
from .xarray_container import RemoteXarray

import intake.container

intake.registry['databroker-remote-xarray'] = RemoteXarray
intake.container.container_map['databroker-xarray'] = RemoteXarray
