from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time as ttime
import datetime

import pytz
from nose.tools import assert_equal, assert_raises, raises
import metadatastore.commands as mdsc
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore.examples.sample_data import temperature_ramp
from metadatastore.api import (find_run_starts, find_run_stops,
                               find_event_descriptors, find_beamline_configs,
                               find_events)
from itertools import product
import logging
loglevel = logging.DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(loglevel)
handler = logging.StreamHandler()
handler.setLevel(loglevel)
logger.addHandler(handler)

# some useful globals
blc_uid = None
run_start_uid = None
document_insertion_time = None
descriptor_uid = None

#### Nose setup/teardown methods ###############################################

def teardown():
    mds_teardown()


def setup():
    mds_setup()
    global blc_uid, run_start_uid, document_insertion_time, run_start
    global descriptor_uid
    document_insertion_time = ttime.time()
    temperature_ramp.run()
    blc_uid = mdsc.insert_beamline_config({}, time=document_insertion_time)
    run_start_uid = mdsc.insert_run_start(scan_id=030213,
                                          beamline_id='testbed',
                                          beamline_config=blc_uid,
                                          owner='tester',
                                          group='awesome-devs',
                                          project='Nikea',
                                          time=document_insertion_time)


def test_document_funcs_for_smoke():
    global run_start_uid, descriptor_uid
    # todo this next line will break once NSLS-II/metadatastore#142 is merged
    run_start, = find_run_starts(uid=run_start_uid.uid)
    descriptors = [desc for desc in find_event_descriptors(uid=descriptor_uid)]
    documents = [run_start]
    documents.extend(descriptors)
    attrs = ['__repr__', '__str__', ]
    for doc, attr in product(documents, attrs):
        getattr(doc, attr)


# todo def test_from_mongo():

# todo def test_from_dict():

if __name__ == '__main__':
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
