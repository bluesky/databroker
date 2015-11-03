from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from collections import deque
import time as ttime
import datetime

import pytz
from nose.tools import assert_equal, assert_raises, raises, assert_true

import metadataclient.commands as mdsc
from metadataclient.testing_utils import mds_setup, mds_teardown
from metadataclient.sample_data import temperature_ramp
import uuid


run_start_uid = None
document_insertion_time = None


def teardown():
    pass
    mds_teardown()


def setup():
    mds_setup()
    global run_start_uid, document_insertion_time
    document_insertion_time = ttime.time()
    temperature_ramp.run()

    run_start_uid = mdsc.insert_run_start(scan_id=3022013,
                                        beamline_id='testbed',
                                        owner='tester',
                                        group='awesome-devs',
                                        project='Nikea',
                                        time=document_insertion_time,
                                        uid=str(uuid.uuid4()))



def setup_syn(custom=None):
    if custom is None:
        custom = {}
    data_keys = {k: {'source': k,
                     'dtype': 'number',
                     'shape': None} for k in 'ABCEDEFGHIJKL'
                 }
    scan_id = 1

    # Create a BeginRunEvent that serves as entry point for a run
    rs = mdsc.insert_run_start(scan_id=scan_id, beamline_id='testing',
                              time=ttime.time(),
                              custom=custom, uid=str(uuid.uuid4()))

    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)
    e_desc = mdsc.insert_descriptor(data_keys=data_keys,
                                    time=ttime.time(),
                                    run_start=rs, uid=str(uuid.uuid4()))
    return rs, e_desc, data_keys


def syn_data(data_keys, count):
    all_data = deque()
    for seq_num in range(count):
        data = {k: float(seq_num) for k in data_keys}
        timestamps = {k: ttime.time() for k in data_keys}

        _time = ttime.time()
        uid = str(uuid.uuid4())
        all_data.append({'data': data, 'timestamps': timestamps,
                         'seq_num': seq_num, 'time': _time,
                         'uid': uid})
    return all_data

@raises(KeyError)
def check_for_id(document):
    """Make sure that our documents do not have an id field
    Parameters
    ----------
    document : metadatastore.document.Document
        A sanitized mongoengine document
    """
    document['id']


def test_event_descriptor_insertion():
    # format some data keys for insertion
    data_keys = {'some_value': {'source': 'PV:pv1',
                                'shape': [1, 2],
                                'dtype': 'array'},
                 'some_other_val': {'source': 'PV:pv2',
                                    'shape': [],
                                    'dtype': 'number'},
                 'data_key3': {'source': 'PV:pv1',
                               'shape': [],
                               'dtype': 'number',
                               'external': 'FS:foobar'}}
    time = ttime.time()
    # test insert
    ev_desc_uid = mdsc.insert_descriptor(run_start_uid, data_keys, time,
                                         str(uuid.uuid4()))
    ev_desc_mds, = mdsc.find_descriptors(uid=ev_desc_uid)
    # make sure the sanitized event descriptor has no uid
    check_for_id(ev_desc_mds)

    # make sure the event descriptor is pointing to the correct run start
    referenced_run_start = ev_desc_mds['run_start']
    assert_equal(referenced_run_start.uid, run_start_uid)
    assert_equal(ev_desc_mds['time'], time)

    for k in data_keys:
        for ik in data_keys[k]:
            assert_equal(ev_desc_mds.data_keys[k][ik],
                         data_keys[k][ik])


def test_insert_run_start():
    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    custom = {'foo': 'bar', 'baz': 42,
              'aardvark': ['ants', 3.14]}
    run_start_uid = mdsc.insert_run_start(
        time, beamline_id=beamline_id,
        scan_id=scan_id, custom=custom, uid=str(uuid.uuid4()))

    run_start_mds, = mdsc.find_run_starts(uid=run_start_uid)

    names = ['time', 'beamline_id', 'scan_id'] + list(custom.keys())
    values = [time, beamline_id, scan_id] + list(custom.values())

    for name, val in zip(names, values):
        assert_equal(getattr(run_start_mds, name), val)

    # make sure the metadatstore document raises properly
    check_for_id(run_start_mds)


def test_run_stop_insertion():
    """Test, uh, the insertion of run stop documents
    """
    run_start_uid = mdsc.insert_run_start(
        time=ttime.time(), beamline_id='sample_beamline', scan_id=42,
        uid=str(uuid.uuid4()))
    time = ttime.time()
    exit_status = 'success'
    reason = 'uh, because this is testing and it better be a success?'
    # insert the document
    run_stop_uid = mdsc.insert_run_stop(run_start_uid, time,
                                       exit_status=exit_status,
                                       reason=reason, uid=str(uuid.uuid4()))

    # get the sanitized run_stop document from metadatastore
    run_stop, = mdsc.find_run_stops(uid=run_stop_uid)

    # make sure it does not have an 'id' field
    check_for_id(run_stop)
    # make sure the run stop is pointing to the correct run start
    referenced_run_start = run_stop['run_start']
    assert_equal(referenced_run_start.uid, run_start_uid)

    # check the remaining fields
    comparisons = {'time': time,
                   'exit_status': exit_status,
                   'reason': reason,
                   'uid': run_stop_uid}
    for attr, known_value in comparisons.items():
        assert_equal(known_value, getattr(run_stop, attr))