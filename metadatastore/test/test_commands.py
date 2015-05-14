from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import bson
import six
import time as ttime
import datetime
import pytz
from ..api import Document as Document

from nose.tools import assert_equal, assert_raises, raises


from metadatastore.odm_templates import (BeamlineConfig, EventDescriptor,
                                         RunStart, RunStop)
import metadatastore.commands as mdsc
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore.examples.sample_data import temperature_ramp

blc_uid = None


def setup():
    mds_setup()
    global blc
    temperature_ramp.run()
    blc_uid = mdsc.insert_beamline_config({}, ttime.time())


def teardown():
    mds_teardown()


def _blc_tester(config_dict):
    """Test BeamlineConfig Insert
    """
    blc_uid = mdsc.insert_beamline_config(config_dict, ttime.time())
    blc, = mdsc.find_beamline_configs(uid=blc_uid)
    assert_equal(blc.uid, blc_uid)
    if config_dict is None:
        config_dict = dict()
    assert_equal(config_dict, blc.config_params)
    return blc


def test_blc_insert():
    for cfd in [None, {}, {'foo': 'bar', 'baz': 5, 'biz': .05}]:
        yield _blc_tester, cfd


def _ev_desc_tester(run_start, data_keys, time):
    ev_desc = mdsc.insert_event_descriptor(run_start_uid,
                                           data_keys, time)
    q_ret, = mdsc.find_event_descriptors(run_start=run_start)
    ret = EventDescriptor.objects.get(run_start_id=run_start.id)
    ret.select_related()
    assert_equal(bson.ObjectId(q_ret.id), ret.id)
    q_ret2, = mdsc.find_event_descriptors(_id=ev_desc.id)
    assert_equal(bson.ObjectId(q_ret2.id), ev_desc.id)

    # Check contents of record we just inserted.
    for name, val in zip(['run_start', 'time'], [run_start, time]):
        assert_equal(getattr(ret, name), val)

    for k in data_keys:
        for ik in data_keys[k]:
            assert_equal(getattr(ret.data_keys[k], ik),
                         data_keys[k][ik])

    # Exercise documented Parameters.
    mdsc.find_event_descriptors(run_start=run_start)
    mdsc.find_event_descriptors(run_start_id=run_start.id)
    mdsc.find_event_descriptors(run_start_id=str(run_start.id))
    mdsc.find_event_descriptors(start_time=ttime.time())
    mdsc.find_event_descriptors(stop_time=ttime.time())
    mdsc.find_event_descriptors(start_time=ttime.time() - 1,
                                stop_time=ttime.time())
    mdsc.find_event_descriptors(uid='foo')
    mdsc.find_event_descriptors(_id=ev_desc.id)
    mdsc.find_event_descriptors(_id=str(ev_desc.id))
    return ev_desc


def test_ev_desc():
    rs_uid = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline',
                                scan_id=42,
                                beamline_config=blc_uid)
    rs = mdsc.find_run_start(uid=rs_uid)
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
    yield _ev_desc_tester, rs, data_keys, time


@raises(mdsc.EventDescriptorIsNoneError)
def test_ev_insert_fail():
    mdsc.insert_event(None, ttime.time(), data={'key': [0, 0]}, seq_num=0)


@raises(ValueError)
def test_proper_data_format():
    data = {'key': [15, ]}
    mdsc._validate_data(data)


def test_dict_key_replace_rt():
    test_d = {'a.b': 1, 'b': .5, 'c.d.e': None}
    src_in, dst_in = mdsc._src_dst('in')
    test_d_in = mdsc._replace_dict_keys(test_d, src_in, dst_in)
    src_out, dst_out = mdsc._src_dst('out')
    test_d_out = mdsc._replace_dict_keys(test_d_in, src_out, dst_out)
    assert_equal(test_d_out, test_d)


def test_src_dst_fail():
    assert_raises(ValueError, mdsc._src_dst, 'aardvark')


def _run_start_tester(time, beamline_id, scan_id):

    run_start_uid = mdsc.insert_run_start(time, beamline_id=beamline_id,
                                      scan_id=scan_id,
                                      beamline_config=blc_uid)
    run_start = find_run_starts(uid=run_start_uid)[0]
    q_ret, = mdsc.find_run_starts(_id=run_start.id)
    assert_equal(bson.ObjectId(q_ret.id), run_start.id)

    # test enhancement by @ericdill b812d6
    q_ret, = mdsc.find_run_starts(_id=str(run_start.id))
    assert_equal(bson.ObjectId(q_ret.id), run_start.id)
    q_ret, = mdsc.find_run_starts(uid=run_start.uid)
    assert_equal(bson.ObjectId(q_ret.id), run_start.id)

    # Check that Document creation does not error.
    Document.from_mongo(run_start)

    # Check contents of record we just inserted.
    ret = RunStart.objects.get(id=run_start.id)

    for name, val in zip(['time', 'beamline_id', 'scan_id'],
                         [time, beamline_id, scan_id]):
        assert_equal(getattr(ret, name), val)

    # Exercise documented kwargs
    mdsc.find_run_starts(limit=5)
    mdsc.find_run_starts(start_time=ttime.time())
    mdsc.find_run_starts(start_time='2015')
    mdsc.find_run_starts(start_time='2015-03-30')
    mdsc.find_run_starts(start_time='2015-03-30 03:00:00')
    mdsc.find_run_starts(start_time=datetime.datetime.now())
    mdsc.find_run_starts(stop_time=ttime.time())
    mdsc.find_run_starts(start_time=ttime.time() - 1, stop_time=ttime.time())
    mdsc.find_run_starts(beamline_id='csx')
    mdsc.find_run_starts(project='world-domination')
    mdsc.find_run_starts(owner='drdrake')
    mdsc.find_run_starts(scan_id=1)
    mdsc.find_run_starts(uid='foo')


def test_run_start():
    time = ttime.time()
    beamline_id = 'sample_beamline'
    yield _run_start_tester, time, beamline_id, 42


def _run_start_with_cfg_tester(beamline_cfg, time, beamline_id, scan_id):
    run_start = mdsc.insert_run_start(time, beamline_id=beamline_id,
                                      beamline_config=beamline_cfg.uid,
                                      scan_id=scan_id)
    Document.from_mongo(run_start)
    ret = RunStart.objects.get(id=run_start.id)

    for name, val in zip(['time', 'beamline_id', 'scan_id', 'beamline_config'],
                         [time, beamline_id, scan_id, beamline_cfg]):
        assert_equal(getattr(ret, name), val)


def test_run_start2():
    bcfg_uid = mdsc.insert_beamline_config({'cfg1': 1}, ttime.time())
    bcfg, = mdsc.find_beamline_configs(uid=bcfg_uid)
    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    yield _run_start_with_cfg_tester, bcfg, time, beamline_id, scan_id


def _event_tester(descriptor, seq_num, data, time):
    pass


def _run_stop_tester(run_start, time):
    run_stop = mdsc.insert_run_stop(run_start.uid, time)

    # run_stop is a mongo document, so this .id is an ObjectId
    q_ret, = mdsc.find_run_stops(_id=run_stop.id)
    assert_equal(bson.ObjectId(q_ret.id), run_stop.id)

    # tests bug fixed by @ericdill in c8befa
    q_ret, = mdsc.find_run_stops(_id=str(run_stop.id))
    assert_equal(bson.ObjectId(q_ret.id), run_stop.id)

    # tests bug fixed by @ericdill 1a167a
    q_ret, = mdsc.find_run_stops(run_start=run_start)
    assert_equal(bson.ObjectId(q_ret.id), run_stop.id)

    q_ret, = mdsc.find_run_stops(run_start_id=run_start.id)
    assert_equal(bson.ObjectId(q_ret.id), run_stop.id)

    q_ret, = mdsc.find_run_stops(run_start_id=str(run_start.id))
    assert_equal(bson.ObjectId(q_ret.id), run_stop.id)

    # Check that Document conversion does not raise.
    Document.from_mongo(run_stop)

    # Check the contents of the record we just inserted.
    ret = RunStop.objects.get(id=run_stop.id)
    for name, val in zip(['id', 'time', 'run_start'],
                         [run_stop.id, time, run_start]):
        assert_equal(getattr(ret, name), val)

    # Exercise documented kwargs
    mdsc.find_run_stops(start_time=ttime.time())
    mdsc.find_run_stops(stop_time=ttime.time())
    mdsc.find_run_stops(start_time=ttime.time() - 1, stop_time=ttime.time())
    mdsc.find_run_stops(reason='whimsy')
    mdsc.find_run_stops(exit_status='success')
    mdsc.find_run_stops(uid='foo')


def test_run_stop():
    rs = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline', scan_id=42,
                                beamline_config=blc_uid)
    time = ttime.time()
    yield _run_stop_tester, rs, time


def test_run_start_custom():
    # Test that Run Start is a DynamicDocument that accepts
    # arbitrary fields.
    cust = {'foo': 'bar', 'baz': 42,
            'aardvark': ['ants', 3.14]}
    rs = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline',
                                scan_id=42,
                                beamline_config=blc_uid,
                                custom=cust)
    rs = find_run_starts(uid=rs)[0]
    Document.from_mongo(rs)
    ret = RunStart.objects.get(id=rs.id)

    for k in cust:
        assert_equal(getattr(ret, k), cust[k])


def _normalize_human_friendly_time_tester(val, should_succeed, etype):
    if isinstance(val, tuple):
        (val, check_output) = val

    if should_succeed:
        output = mdsc._normalize_human_friendly_time(val)
        assert(isinstance(output, float))
        try:
            assert_equal(output, check_output)
        except NameError:
            pass
    else:
        assert_raises(etype, mdsc._normalize_human_friendly_time, val)


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
        yield _normalize_human_friendly_time_tester, val, False, pytz.NonExistentTimeError


    bad_test_values = ['2015-11-01 01:00:00',
                       '2015-11-01 01:59:59']
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, pytz.AmbiguousTimeError


    bad_test_values = ['2015-04-15 03:',
                       str(ttime.time()),
                       'aardvark',
                       ]
    for val in bad_test_values:
        yield _normalize_human_friendly_time_tester, val, False, ValueError


# smoketests

def test_find_last_for_smoke():
    mdsc.find_last()
