from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import warnings
from collections import deque
import time as ttime
import datetime

import pytz
import pytest
import metadatastore.commands as mdsc
import metadatastore.core as core
from .utils import mds_setup, mds_teardown
from metadatastore.examples.sample_data import temperature_ramp
import uuid

# some useful globals
run_start_uid = None
document_insertion_time = None


# ### Nose setup/teardown methods #############################################


def teardown_module(module):
    pass
    mds_teardown()


def setup_module(module):
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

# ### Helper functions ########################################################


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
                               uid=str(uuid.uuid4()),
                               **custom)

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


# ### Testing metadatastore insertion functionality ###########################


def check_for_id(document):
    """Make sure that our documents do not have an id field

    Parameters
    ----------
    document : metadatastore.document.Document
        A sanitized mongoengine document
    """
    with pytest.raises(KeyError):
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
    assert referenced_run_start.uid == run_start_uid
    assert ev_desc_mds['time'] == time

    for k in data_keys:
        for ik in data_keys[k]:
            assert ev_desc_mds.data_keys[k][ik] == data_keys[k][ik]


def test_custom_warn():

    run_start_uid = str(uuid.uuid4())

    with warnings.catch_warnings(record=True) as w:
        run_start_uid = mdsc.insert_run_start(
            scan_id=30220, beamline_id='testbed',
            owner='Al the Aardvark', group='Orycteropus',
            project='Nikea', time=document_insertion_time,
            uid=run_start_uid, custom={'order': 'Tubulidentata'})
        assert len(w) == 1

    rs = next(mdsc.find_run_starts(order='Tubulidentata'))
    assert rs['uid'] == run_start_uid

    with warnings.catch_warnings(record=True) as w:
        ev_desc_uid = mdsc.insert_descriptor(
            run_start_uid,
            {'a': {'source': 'zoo', 'shape': [], 'dtype': 'number'}},
            ttime.time(), str(uuid.uuid4()), custom={'food': 'ants'})

        assert len(w) == 1
    ed = mdsc.descriptor_given_uid(ev_desc_uid)
    assert ed['food'] == 'ants'

    with warnings.catch_warnings(record=True) as w:
        stop_uid = str(uuid.uuid4())
        mdsc.insert_run_stop(run_start_uid, ttime.time(),
                             stop_uid, custom={'navy': 'VF-114'})

        assert len(w) == 1

    run_stop = mdsc.run_stop_given_uid(stop_uid)
    assert run_stop['navy'] == 'VF-114'


def test_insert_run_start():
    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    custom = {'foo': 'bar', 'baz': 42,
              'aardvark': ['ants', 3.14]}
    run_start_uid = mdsc.insert_run_start(
        time, beamline_id=beamline_id,
        scan_id=scan_id, uid=str(uuid.uuid4()), **custom)

    run_start_mds, = mdsc.find_run_starts(uid=run_start_uid)

    names = ['time', 'beamline_id', 'scan_id'] + list(custom.keys())
    values = [time, beamline_id, scan_id] + list(custom.values())

    for name, val in zip(names, values):
        assert getattr(run_start_mds, name) == val

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
    assert referenced_run_start.uid == run_start_uid

    # check the remaining fields
    comparisons = {'time': time,
                   'exit_status': exit_status,
                   'reason': reason,
                   'uid': run_stop_uid}
    for attr, known_value in comparisons.items():
        assert known_value == getattr(run_stop, attr)


def test_find_events_smoke():

    num = 50
    rs, e_desc, data_keys = setup_syn()
    all_data = syn_data(data_keys, num)

    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))
    mdsc.clear_process_cache()

    # make sure the uid works
    next(mdsc.find_events(descriptor=e_desc))

    mdsc.clear_process_cache()
    descriptor, = mdsc.find_descriptors(uid=e_desc)

    mdsc.clear_process_cache()
    # make sure that searching by descriptor document works
    next(mdsc.find_events(descriptor=descriptor))

def test_find_events_ValueError():
    with pytest.raises(ValueError):
        list(mdsc.find_events(event_descriptor='cat'))


def test_bad_bulk_insert_event_data():
    num = 50
    rs, e_desc, data_keys = setup_syn()
    all_data = syn_data(data_keys, num)

    # remove one of the keys from the event data
    del all_data[-1]['data']['E']
    with pytest.raises(ValueError):
        mdsc.bulk_insert_events(e_desc, all_data, validate=True)


def test_bad_bulk_insert_event_timestamp():
    """Test what happens when one event is missing a timestamp for one key"""
    num = 50
    rs, e_desc, data_keys = setup_syn()
    all_data = syn_data(data_keys, num)
    # remove one of the keys from the event timestamps
    del all_data[1]['timestamps']['F']
    with pytest.raises(ValueError):
        mdsc.bulk_insert_events(e_desc, all_data, validate=True)


def test_no_evdesc():
    run_start_uid = mdsc.insert_run_start(
        scan_id=42, beamline_id='testbed', owner='tester',
        group='awesome-devs', project='Nikea', time=document_insertion_time,
        uid=str(uuid.uuid4()))

    with pytest.raises(mdsc.NoEventDescriptors):
        mdsc.descriptors_by_start(run_start_uid)


# ### Testing metadatastore find functionality ################################
def _find_helper(func, kw):
    # dereference the generator...
    list(func(**kw))


def test_find_funcs_for_smoke():
    rs, = mdsc.find_run_starts(uid=run_start_uid)
    test_dict = {
        mdsc.find_run_starts: [
            {'limit': 5},
            {'start_time': ttime.time()},
            {'start_time': '2015'},
            {'start_time': '2015-03-30'},
            {'start_time': '2015-03-30 03:00:00'},
            {'start_time': datetime.datetime.now()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time() - 1, 'stop_time': ttime.time()},
            {'beamline_id': 'csx'},
            {'project': 'world-domination'},
            {'owner': 'drdrake'},
            {'scan_id': 1},
            {'uid': run_start_uid}],
        mdsc.find_run_stops: [
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time()-1, 'stop_time': ttime.time()},
            {'reason': 'whimsy'},
            {'exit_status': 'success'},
            {'run_start': rs},
            {'run_start_uid': rs.uid},
            {'uid': 'foo'}],
        mdsc.find_descriptors: [
            {'run_start': rs},
            {'run_start': rs.uid},
            {'start_time': ttime.time()},
            {'stop_time': ttime.time()},
            {'start_time': ttime.time() - 1, 'stop_time': ttime.time()},
            {'uid': 'foo'}],
    }
    for func, list_o_dicts in test_dict.items():
        for dct in list_o_dicts:
            yield _find_helper, func, dct


# ### Test metadatastore time formatting ######################################


def _normalize_human_friendly_time_tester(val, should_succeed, etype):
    if isinstance(val, tuple):
        (val, check_output) = val

    if should_succeed:
        output = core._normalize_human_friendly_time(val, 'US/Eastern')
        assert(isinstance(output, float))
        try:
            assert output == check_output
        except NameError:
            pass
    else:
        with pytest.raises(etype):
            core._normalize_human_friendly_time(val, 'US/Eastern')


def test_normalize_human_friendly_time():
    # should get tz from conf?  but no other tests get conf stuff...
    zone = pytz.timezone('US/Eastern')

    good_test_values = [('2014', 1388552400.0),
                        ('2014 ', 1388552400.0),
                        ('2014-02', 1391230800.0),
                        ('2014-02 ', 1391230800.0),
                        ('2014-2', 1391230800.0),
                        ('2014-2 ', 1391230800.0),
                        ('2014-2-10', 1392008400.0),
                        ('2014-2-10 ', 1392008400.0),
                        ('2014-02-10', 1392008400.0),
                        ('2014-02-10 ', 1392008400.0),
                        (' 2014-02-10 10 ', 1392044400.0),
                        ('2014-02-10 10:1', 1392044460.0),
                        ('2014-02-10 10:1 ', 1392044460.0),
                        ('2014-02-10 10:1:00', 1392044460.0),
                        ('2014-02-10 10:01:00', 1392044460.0),

                        # dst transistion tests
                        ('2015-03-08 01:59:59', 1425797999.0),  # is_dst==False
                        # at 2am, spring forward to 3am.
                        # [02:00:00 - 02:59:59] does not exist
                        ('2015-03-08 03:00:00', 1425798000.0),  # is_dst==True

                        ('2015-11-01 00:59:59', 1446353999.0),  # is_dst==True
                        # at 2am, fall back to 1am
                        # [01:00:00-01:59:59] is ambiguous without is_dst
                        ('2015-11-01 02:00:00', 1446361200.0),  # is_dst==False

                        # other
                        ttime.time(),
                        datetime.datetime.now(),
                        zone.localize(datetime.datetime.now()),
                        ]
    for val in good_test_values:
        yield _normalize_human_friendly_time_tester, val, True, None

    bad_test_values = ['2015-03-08 02:00:00',
                       '2015-03-08 02:59:59']
    for val in bad_test_values:
        yield (_normalize_human_friendly_time_tester,
               val, False, pytz.NonExistentTimeError)

    bad_test_values = ['2015-11-01 01:00:00',
                       '2015-11-01 01:59:59']
    for val in bad_test_values:
        yield (_normalize_human_friendly_time_tester,
               val, False, pytz.AmbiguousTimeError)

    bad_test_values = ['2015-04-15 03:',
                       str(ttime.time()),
                       'aardvark',
                       ]
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, ValueError


def test_bulk_insert():
    num = 50
    rs, e_desc, data_keys = setup_syn()
    all_data = syn_data(data_keys, num)

    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    ev_gen = mdsc.get_events_generator(e_desc)

    for ret, expt in zip(ev_gen, all_data):
        assert ret['descriptor']['uid'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]


def test_bulk_table():
    num = 50
    rs, e_desc, data_keys = setup_syn()
    all_data = syn_data(data_keys, num)

    mdsc.bulk_insert_events(e_desc, all_data, validate=False)
    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))
    ret = mdsc.get_events_table(e_desc)
    descriptor, data_table, seq_nums, times, uids, timestamps_table = ret

    for vals in data_table.values():
        assert all(s == v for s, v in zip(seq_nums, vals))


def test_cache_clear_lookups():
    run_start_uid, e_desc_uid, data_keys = setup_syn()
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


def test_run_stop_by_run_start():
    run_start_uid, e_desc_uid, data_keys = setup_syn()
    run_stop_uid = mdsc.insert_run_stop(run_start_uid,
                                        ttime.time(), uid=str(uuid.uuid4()))
    run_start = mdsc.run_start_given_uid(run_start_uid)
    run_stop = mdsc.run_stop_given_uid(run_stop_uid)
    ev_desc = mdsc.descriptor_given_uid(e_desc_uid)

    run_stop2 = mdsc.stop_by_start(run_start)
    run_stop3 = mdsc.stop_by_start(run_start_uid)
    assert run_stop == run_stop2
    assert run_stop == run_stop3

    ev_desc2, = mdsc.descriptors_by_start(run_start)
    ev_desc3, = mdsc.descriptors_by_start(run_start_uid)
    assert ev_desc == ev_desc2
    assert ev_desc == ev_desc3


def test_find_run_start():
    run_start_uid, e_desc_uid, data_keys = setup_syn()
    mdsc.insert_run_stop(run_start_uid, ttime.time(), uid=str(uuid.uuid4()))

    run_start = mdsc.run_start_given_uid(run_start_uid)

    run_start2, = list(mdsc.find_run_starts(uid=run_start_uid))

    assert run_start == run_start2


def test_find_run_stop():
    run_start_uid, e_desc_uid, data_keys = setup_syn()
    run_stop_uid = mdsc.insert_run_stop(run_start_uid, ttime.time(),
                                        uid=str(uuid.uuid4()))

    run_start = mdsc.run_start_given_uid(run_start_uid)
    run_stop = mdsc.run_stop_given_uid(run_stop_uid)

    run_stop2, = list(mdsc.find_run_stops(run_start=run_start_uid))
    run_stop3, = list(mdsc.find_run_stops(run_start=run_start))
    assert run_stop == run_stop2
    assert run_stop == run_stop3


def test_double_run_stop():
    run_start_uid, e_desc_uid, data_keys = setup_syn()
    mdsc.insert_run_stop(run_start_uid, ttime.time(),
                         uid=str(uuid.uuid4()))
    with pytest.raises(RuntimeError):
        mdsc.insert_run_stop(run_start_uid, ttime.time(),
                             uid=str(uuid.uuid4()))


def test_find_last_for_smoke():
    last, = mdsc.find_last()


def test_fail_runstart():
    with pytest.raises(mdsc.NoRunStart):
        mdsc.run_start_given_uid('aardvark')


def test_bad_event_desc():

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
