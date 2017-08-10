from collections import deque
import pickle
import time as ttime
import uuid
import os
import pytest
import warnings

import numpy as np
from types import GeneratorType


def check_for_id(document):
    """Make sure that our documents do not have an id field

    Parameters
    ----------
    document : metadatastore.document.Document
        A sanitized mongoengine document
    """
    with pytest.raises(KeyError):
        document['id']
    with pytest.raises(KeyError):
        document['_id']


def setup_syn(mds, custom=None):
    if custom is None:
        custom = {}
    data_keys = {k: {'source': k,
                     'dtype': 'number',
                     'shape': None} for k in 'ABCEDEFGHIJKL'
                 }
    data_keys['Z'] = {'source': 'Z', 'dtype': 'array', 'shape': [5, 5],
                      'external': 'foo'}
    scan_id = 1

    # Create a BeginRunEvent that serves as entry point for a run
    rs = mds.insert_run_start(scan_id=scan_id, beamline_id='testing',
                              time=ttime.time(),
                              uid=str(uuid.uuid4()),
                              **custom)

    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)
    e_desc = mds.insert_descriptor(data_keys=data_keys,
                                   time=ttime.time(),
                                   run_start=rs, uid=str(uuid.uuid4()))
    return rs, e_desc, data_keys


def syn_data(data_keys, count, is_np=False):
    all_data = deque()
    for seq_num in range(count):
        data = {k: float(seq_num) for k in data_keys}
        timestamps = {k: ttime.time() for k in data_keys}

        _time = ttime.time()
        uid = str(uuid.uuid4())
        if is_np:
            all_data.append({'data': data, 'timestamps': timestamps,
                         'seq_num': seq_num, 'time': _time,
                         'uid': uid})
        else:
            all_data.append({'data': data, 'timestamps': timestamps,
                             'seq_num': seq_num, 'time': _time,
                             'uid': uid})
    return all_data


def test_pickle(mds_all):
    md = mds_all
    md2 = pickle.loads(pickle.dumps(md))

    assert md.config == md2.config


def test_event_descriptor_insertion(mds_all):
    mds = mds_all
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

    document_insertion_time = ttime.time()
    run_start_uid = mds.insert_run_start(scan_id=3022013,
                                         beamline_id='testbed',
                                         owner='tester',
                                         group='awesome-devs',
                                         project='Nikea',
                                         time=document_insertion_time,
                                         uid=str(uuid.uuid4()))
    time = ttime.time()
    # test insert
    ev_desc_uid = mds.insert_descriptor(run_start_uid, data_keys, time,
                                        str(uuid.uuid4()))
    ev_desc_mds, = mds.find_descriptors(uid=ev_desc_uid)
    # make sure the sanitized event descriptor has no uid
    check_for_id(ev_desc_mds)

    # make sure the event descriptor is pointing to the correct run start
    referenced_run_start = ev_desc_mds['run_start']
    assert referenced_run_start == run_start_uid
    assert ev_desc_mds['time'] == time

    for k in data_keys:
        for ik in data_keys[k]:
            assert ev_desc_mds['data_keys'][k][ik] == data_keys[k][ik]


def test_custom_warn(mds_all):
    mds = mds_all
    run_start_uid = str(uuid.uuid4())
    warnings.simplefilter('always', UserWarning)
    with pytest.warns(UserWarning):
        run_start_uid = mds.insert_run_start(
            scan_id=30220, beamline_id='testbed',
            owner='Al the Aardvark', group='Orycteropus',
            project='Nikea', time=ttime.time(),
            uid=run_start_uid, custom={'order': 'Tubulidentata'})

    rs = next(mds.find_run_starts(order='Tubulidentata'))
    assert rs['uid'] == run_start_uid

    with warnings.catch_warnings(record=True) as w:
        ev_desc_uid = mds.insert_descriptor(
            run_start_uid,
            {'a': {'source': 'zoo', 'shape': [], 'dtype': 'number'}},
            ttime.time(), str(uuid.uuid4()), custom={'food': 'ants'})

        assert len(w) == 1
    ed = mds.descriptor_given_uid(ev_desc_uid)
    assert ed['food'] == 'ants'

    with warnings.catch_warnings(record=True) as w:
        stop_uid = str(uuid.uuid4())
        mds.insert_run_stop(run_start_uid, ttime.time(),
                            stop_uid, custom={'navy': 'VF-114'})

        assert len(w) == 1

    run_stop = mds.run_stop_given_uid(stop_uid)
    assert run_stop['navy'] == 'VF-114'


def test_insert_run_start(mds_all):
    mds = mds_all

    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    custom = {'foo': 'bar', 'baz': 42,
              'aardvark': ['ants', 3.14]}
    run_start_uid = mds.insert_run_start(
        time, beamline_id=beamline_id,
        scan_id=scan_id, uid=str(uuid.uuid4()), **custom)
    assert isinstance(run_start_uid, str)
    run_start_mds, = mds.find_run_starts(uid=run_start_uid)
    assert isinstance(run_start_mds, dict)
    names = ['time', 'beamline_id', 'scan_id'] + list(custom.keys())
    values = [time, beamline_id, scan_id] + list(custom.values())

    for name, val in zip(names, values):
        assert run_start_mds[name] == val

    # make sure the metadatstore document raises properly
    check_for_id(run_start_mds)


def test_run_stop_insertion(mds_all):
    """Test, uh, the insertion of run stop documents
    """
    mds = mds_all
    run_start_uid = mds.insert_run_start(
        time=ttime.time(), beamline_id='sample_beamline', scan_id=42,
        uid=str(uuid.uuid4()))
    time = ttime.time()
    exit_status = 'success'
    reason = 'uh, because this is testing and it better be a success?'
    # insert the document
    run_stop_uid = mds.insert_run_stop(run_start_uid, time,
                                       exit_status=exit_status,
                                       reason=reason, uid=str(uuid.uuid4()))

    # get the sanitized run_stop document from metadatastore
    run_stop, = mds.find_run_stops(uid=run_stop_uid)
    assert isinstance(run_stop_uid, str)
    assert isinstance(run_stop, dict)
    # make sure it does not have an 'id' field
    check_for_id(run_stop)
    # make sure the run stop is pointing to the correct run start
    referenced_run_start = run_stop['run_start']
    assert referenced_run_start == run_start_uid

    # check the remaining fields
    comparisons = {'time': time,
                   'exit_status': exit_status,
                   'reason': reason,
                   'uid': run_stop_uid}
    for attr, known_value in comparisons.items():
        assert known_value == run_stop[attr]


def test_find_events_smoke(mds_all):
    mds = mds_all
    if not hasattr(mds, 'find_events'):
        pytest.skip("This mds flavor does not have find_events")
    num = 50
    rs, e_desc, data_keys = setup_syn(mds)
    all_data = syn_data(data_keys, num)

    mds.bulk_insert_events(e_desc, all_data, validate=False)
    mds.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))
    mds.clear_process_cache()

    # make sure the uid works
    next(mds.find_events(descriptor=e_desc))
    mds.clear_process_cache()
    descriptor, = mds.find_descriptors(uid=e_desc)
    mds.clear_process_cache()
    next(mds.find_events(descriptor=descriptor))


def test_find_events_ValueError(mds_all):
    mdsc = mds_all
    if not hasattr(mdsc, 'find_events'):
        pytest.skip("This mds flavor does not have find_events")
    with pytest.raises(ValueError):
        list(mdsc.find_events(event_descriptor='cat'))


def test_bad_bulk_insert_event_data(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    # remove one of the keys from the event data
    del all_data[-1]['data']['E']
    with pytest.raises(ValueError):
        mdsc.bulk_insert_events(e_desc, all_data, validate=True)


def test_sanitize_np(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num, is_np=True)
    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    ev_gen = mdsc.get_events_generator(e_desc)

    for ret, expt in zip(ev_gen, all_data):
        assert ret['descriptor'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
        assert ret['filled'] == {'Z': False}


def test_bad_bulk_insert_event_timestamp(mds_all):
    """Test what happens when one event is missing a timestamp for one key"""
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)
    # remove one of the keys from the event timestamps
    del all_data[1]['timestamps']['F']
    with pytest.raises(ValueError):
        mdsc.bulk_insert_events(e_desc, all_data, validate=True)


def test_no_evdesc(mds_all):
    mdsc = mds_all
    run_start_uid = mdsc.insert_run_start(
        scan_id=42, beamline_id='testbed', owner='tester',
        group='awesome-devs', project='Nikea', time=ttime.time(),
        uid=str(uuid.uuid4()))

    with pytest.raises(mds_all.NoEventDescriptors):
        mdsc.descriptors_by_start(run_start_uid)


def test_bulk_insert(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    ev_gen = mdsc.get_events_generator(e_desc)

    for ret, expt in zip(ev_gen, all_data):
        assert ret['descriptor'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
        assert ret['filled'] == {'Z': False}


def test_iterative_insert(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    for d in all_data:
        mdsc.insert_event(e_desc, **d)

    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    ev_gen = mdsc.get_events_generator(e_desc)
    ret_lag = None
    assert isinstance(ev_gen, GeneratorType)
    for ret, expt in zip(ev_gen, all_data):
        assert ret['descriptor'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
        if ret_lag:
            assert ret['filled'] is not ret_lag['filled']
        ret_lag = ret

def test_iterative_insert_np(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num, is_np=True)

    for d in all_data:
        mdsc.insert_event(e_desc, **d)

    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    ev_gen = mdsc.get_events_generator(e_desc)
    ret_lag = None
    assert isinstance(ev_gen, GeneratorType)
    for ret, expt in zip(ev_gen, all_data):
        assert ret['descriptor'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
        if ret_lag:
            assert ret['filled'] is not ret_lag['filled']
        ret_lag = ret


def test_bulk_table(mds_all):
    mdsc = mds_all
    num = 50
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))
    ret = mdsc.get_events_table(e_desc)
    assert isinstance(ret, tuple)
    descriptor, data_table, seq_nums, times, uids, timestamps_table = ret
    assert isinstance(descriptor, dict)
    assert isinstance(data_table, dict)
    assert isinstance(seq_nums, list)
    assert isinstance(times, list)
    assert isinstance(timestamps_table, dict)
    assert isinstance(uids, list)
    for vals in data_table.values():
        assert all(s == v for s, v in zip(seq_nums, vals))


def test_cache_clear_lookups(mds_all):
    mdsc = mds_all
    run_start_uid, e_desc_uid, data_keys = setup_syn(mdsc)
    run_stop_uid = mdsc.insert_run_stop(run_start_uid,
                                        ttime.time(), uid=str(uuid.uuid4()))
    run_start = mdsc.run_start_given_uid(run_start_uid)
    run_stop = mdsc.run_stop_given_uid(run_stop_uid)
    ev_desc = mdsc.descriptor_given_uid(e_desc_uid)

    mdsc.clear_process_cache()

    run_start2 = mdsc.run_start_given_uid(run_start_uid)
    mdsc.clear_process_cache()

    run_stop2 = mdsc.run_stop_given_uid(run_stop_uid)
    mdsc.clear_process_cache()
    ev_desc2 = mdsc.descriptor_given_uid(e_desc_uid)
    ev_desc3 = mdsc.descriptor_given_uid(e_desc_uid)

    assert run_start == run_start2
    assert run_stop == run_stop2
    assert ev_desc == ev_desc2
    assert ev_desc == ev_desc3


def test_run_stop_by_run_start(mds_all):
    mdsc = mds_all
    run_start_uid, e_desc_uid, data_keys = setup_syn(mdsc)
    run_stop_uid = mdsc.insert_run_stop(run_start_uid,
                                        ttime.time(), uid=str(uuid.uuid4()))
    run_start = mdsc.run_start_given_uid(run_start_uid)
    run_stop = mdsc.run_stop_given_uid(run_stop_uid)
    ev_desc = mdsc.descriptor_given_uid(e_desc_uid)
    assert isinstance(ev_desc, dict)
    assert isinstance(run_start, dict)
    assert isinstance(run_stop, dict)
    assert isinstance(ev_desc, dict)
    run_stop2 = mdsc.stop_by_start(run_start)
    run_stop3 = mdsc.stop_by_start(run_start_uid)
    assert isinstance(run_stop2, dict)
    assert run_stop == run_stop2
    assert run_stop == run_stop3

    ev_desc2, = mdsc.descriptors_by_start(run_start)
    assert isinstance(ev_desc2, dict)
    ev_desc3, = mdsc.descriptors_by_start(run_start_uid)
    assert ev_desc == ev_desc2
    assert ev_desc == ev_desc3


def test_find_run_start(mds_all):
    mdsc = mds_all
    run_start_uid, e_desc_uid, data_keys = setup_syn(mdsc)
    mdsc.insert_run_stop(run_start_uid, ttime.time(), uid=str(uuid.uuid4()))

    run_start = mdsc.run_start_given_uid(run_start_uid)

    run_start2, = list(mdsc.find_run_starts(uid=run_start_uid))

    assert run_start == run_start2


def test_find_run_stop(mds_all):
    mdsc = mds_all
    run_start_uid, e_desc_uid, data_keys = setup_syn(mdsc)
    run_stop_uid = mdsc.insert_run_stop(run_start_uid, ttime.time(),
                                        uid=str(uuid.uuid4()))

    run_start = mdsc.run_start_given_uid(run_start_uid)
    run_stop = mdsc.run_stop_given_uid(run_stop_uid)

    run_stop2, = list(mdsc.find_run_stops(run_start=run_start_uid))
    run_stop3, = list(mdsc.find_run_stops(run_start=run_start))
    assert run_stop == run_stop2
    assert run_stop == run_stop3


def test_double_run_start(mds_all):
    mdsc = mds_all

    custom = {}
    data_keys = {k: {'source': k,
                     'dtype': 'number',
                     'shape': None} for k in 'ABCEDEFGHIJKL'
                 }
    data_keys['Z'] = {'source': 'Z', 'dtype': 'array', 'shape': [5, 5],
                      'external': 'foo'}
    scan_id = 1

    # Create a BeginRunEvent that serves as entry point for a run
    start_dict = dict(scan_id=scan_id, beamline_id='testing',
                      time=ttime.time(),
                      uid=str(uuid.uuid4()),
                      **custom)
    rs = mdsc.insert_run_start(**start_dict)

    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)
    e_desc = mdsc.insert_descriptor(data_keys=data_keys,
                                    time=ttime.time(),
                                    run_start=rs, uid=str(uuid.uuid4()))
    mdsc.insert_run_stop(rs, ttime.time(),
                         uid=str(uuid.uuid4()))
    with pytest.raises(Exception):
        mdsc.insert_run_start(**start_dict)


def test_double_run_stop(mds_all):
    mdsc = mds_all
    run_start_uid, e_desc_uid, data_keys = setup_syn(mdsc)
    mdsc.insert_run_stop(run_start_uid, ttime.time(),
                         uid=str(uuid.uuid4()))
    with pytest.raises(RuntimeError):
        mdsc.insert_run_stop(run_start_uid, ttime.time(),
                             uid=str(uuid.uuid4()))


def test_reset_caches(mds_all):
    # smoke test
    mds_all.reset_caches()


def test_fail_runstart(mds_all):
    mdsc = mds_all
    with pytest.raises(mds_all.NoRunStart):
        mdsc.run_start_given_uid('aardvark')


def test_exceptions_are_mds_attributes(mds_all):
    isinstance(mds_all.NoRunStop, Exception)
    isinstance(mds_all.NoRunStart, Exception)
    isinstance(mds_all.NoEventDescriptors, Exception)


def test_bad_event_desc(mds_all):
    mdsc = mds_all
    data_keys = {k: {'source': k,
                     'dtype': 'number',
                     'shape': None} for k in ['foo', 'foo.bar']
                 }
    scan_id = 1

    # Create a BeginRunEvent that serves as entry point for a run
    rs = mdsc.insert_run_start(scan_id=scan_id, beamline_id='testing',
                               time=ttime.time(),
                               uid=str(uuid.uuid4()))

    # Create an EventDescriptor that indicates the data
    # keys and serves as header for set of Event(s)
    with pytest.raises(ValueError):
        mdsc.insert_descriptor(data_keys=data_keys,
                               time=ttime.time(),
                               run_start=rs, uid=str(uuid.uuid4()))


def test_reload(mds_portable):
    if 'hdf5' in type(mds_portable).__module__:
        pytest.xfail('know bug in hdf5 backend')

    num = 5
    mdsc = mds_portable
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    for d in all_data:
        mdsc.insert_event(e_desc, **d)

    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    mds_new = type(mds_portable)(mds_portable.config)

    ev_gen_base = mdsc.get_events_generator(e_desc)
    ev_gen_reloaded = mds_new.get_events_generator(e_desc)

    for ret, ret_n, expt in zip(ev_gen_base, ev_gen_reloaded, all_data):
        assert ret['descriptor'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
            assert ret_n[k] == expt[k]
