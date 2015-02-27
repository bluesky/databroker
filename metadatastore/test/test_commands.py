from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import uuid
import time as ttime
from ..api import Document as Document

from nose.tools import make_decorator
from nose.tools import assert_equal, assert_raises


from metadatastore.odm_templates import (BeamlineConfig, EventDescriptor,
                                         Event, RunStart, RunStop)
import metadatastore.commands as mdsc
from metadatastore.utils.testing import mds_setup, mds_teardown

blc = None


def setup():
    mds_setup()
    global blc
    blc = mdsc.insert_beamline_config({}, ttime.time())


def teardown():
    mds_teardown()


def _blc_tester(config_dict):
    """Test BeamlineConfig Insert
    """
    blc = mdsc.insert_beamline_config(config_dict, ttime.time())
    doc = Document(blc) # test document creation
    repr(doc) # exercise Document.__repr__
    BeamlineConfig.objects.get(id=blc.id)
    if config_dict is None:
        config_dict = dict()
    assert_equal(config_dict, blc.config_params)
    return blc


def test_blc_insert():
    for cfd in [None, {}, {'foo': 'bar', 'baz': 5, 'biz': .05}]:
        yield _blc_tester, cfd


def _ev_desc_tester(run_start, data_keys, time):
    ev_desc = mdsc.insert_event_descriptor(run_start,
                                           data_keys, time)
    Document(ev_desc) # test document creation
    ret = EventDescriptor.objects.get(id=ev_desc.id)

    for name, val in zip(['run_start', 'time'], [run_start, time]):
        assert_equal(getattr(ret, name), val)

    for k in data_keys:
        for ik in data_keys[k]:
            assert_equal(getattr(ret.data_keys[k], ik),
                         data_keys[k][ik])

    return ev_desc


def test_ev_desc():
    bre = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline',
                                scan_id=42,
                                beamline_config=blc)
    Document(bre) # test document creation
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
    yield _ev_desc_tester, bre, data_keys, time


def test_dict_key_replace_rt():
    test_d = {'a.b': 1, 'b': .5, 'c.d.e': None}
    src_in, dst_in = mdsc.__src_dst('in')
    test_d_in = mdsc.__replace_dict_keys(test_d, src_in, dst_in)
    src_out, dst_out = mdsc.__src_dst('out')
    test_d_out = mdsc.__replace_dict_keys(test_d_in, src_out, dst_out)
    assert_equal(test_d_out, test_d)


def test_src_dst_fail():
    assert_raises(ValueError, mdsc.__src_dst, 'aardvark')


def _run_start_tester(time, beamline_id, scan_id):

    run_start = mdsc.insert_run_start(time, beamline_id, scan_id=scan_id,
                                      beamline_config=blc)
    Document(run_start) # test document creation
    ret = RunStart.objects.get(id=run_start.id)

    for name, val in zip(['time', 'beamline_id', 'scan_id'],
                         [time, beamline_id, scan_id]):
        assert_equal(getattr(ret, name), val)


def test_run_start():
    time = ttime.time()
    beamline_id = 'sample_beamline'
    yield _run_start_tester, time, beamline_id, 42


def _run_start_with_cfg_tester(beamline_cfg, time, beamline_id, scan_id):
    run_start = mdsc.insert_run_start(time, beamline_id,
                                      beamline_config=beamline_cfg,
                                      scan_id=scan_id)
    Document(run_start) # test document creation
    ret = RunStart.objects.get(id=run_start.id)

    for name, val in zip(['time', 'beamline_id', 'scan_id', 'beamline_config'],
                         [time, beamline_id, scan_id, beamline_cfg]):
        assert_equal(getattr(ret, name), val)


def test_run_start2():
    bcfg = mdsc.insert_beamline_config({'cfg1': 1}, ttime.time())
    time = ttime.time()
    beamline_id = 'sample_beamline'
    scan_id = 42
    yield _run_start_with_cfg_tester, bcfg, time, beamline_id, scan_id


def _event_tester(descriptor, seq_num, data, time):
    pass


def _end_run_tester(run_start, time):
    print('br:', run_start)
    end_run = mdsc.insert_run_stop(run_start, time)
    Document(end_run) # test document creation
    ret = RunStop.objects.get(id=end_run.id)
    for name, val in zip(['id', 'time', 'run_start'],
                    [end_run.id, time, run_start]):
        assert_equal(getattr(ret, name), val)


def test_end_run():
    bre = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline', scan_id=42,
                                beamline_config=blc)
    Document(bre) # test document creation
    print('bre:', bre)
    time = ttime.time()
    yield _end_run_tester, bre, time


def test_bre_custom():
    cust = {'foo': 'bar', 'baz': 42,
            'aardvark': ['ants', 3.14]}
    bre = mdsc.insert_run_start(time=ttime.time(),
                                beamline_id='sample_beamline',
                                scan_id=42,
                                beamline_config=blc,
                                custom=cust)
    Document(bre) # test document creation
    ret = RunStart.objects.get(id=bre.id)

    for k in cust:
        assert_equal(getattr(ret, k), cust[k])
