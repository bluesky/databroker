# Smoketest the api

from metadatastore.api import find_events
from metadatastore.api import find_event_descriptors
from metadatastore.api import find_run_starts
from metadatastore.api import find_run_stops
from metadatastore.api import find_last
from metadatastore.api import insert_event
from metadatastore.api import insert_event_descriptor
from metadatastore.api import insert_run_start
from metadatastore.api import insert_run_stop
from metadatastore.api import EventDescriptorIsNoneError
from metadatastore.api import format_data_keys
from metadatastore.api import db_connect
from metadatastore.api import db_disconnect


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
