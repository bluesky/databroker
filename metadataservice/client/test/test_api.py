# Smoketest the api

from metadataservice.client.api import find_events
from metadataservice.client.api import find_descriptors
from metadataservice.client.api import find_run_starts
from metadataservice.client.api import find_run_stops
from metadataservice.client.api import find_last
from metadataservice.client.api import insert_event
from metadataservice.client.api import insert_descriptor
from metadataservice.client.api import insert_run_start
from metadataservice.client.api import insert_run_stop
from metadataservice.client.api import server_connect
# from metadataservice.client.api import db_disconnect


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
