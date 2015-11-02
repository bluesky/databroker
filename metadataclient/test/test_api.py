# Smoketest the api


from metadataclient.api import insert_run_start
from metadataclient.api import insert_run_stop
from metadataclient.api import insert_descriptor
from metadataclient.api import insert_event
from metadataclient.api import find_run_starts
from metadataclient.api import find_run_stops
from metadataclient.api import find_descriptors
from metadataclient.api import find_events
from metadataclient.api import find_last


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
