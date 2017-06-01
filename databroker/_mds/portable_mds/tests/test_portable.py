from databroker.tests.test_mds import setup_syn, syn_data
import uuid
import time as ttime
import pytest


def test_reload(mds_all):
    if 'hdf5' in type(mds_all).__module__:
        pytest.xfail('know bug in hdf5 backend')

    num = 5
    mdsc = mds_all
    rs, e_desc, data_keys = setup_syn(mdsc)
    all_data = syn_data(data_keys, num)

    for d in all_data:
        mdsc.insert_event(e_desc, **d)

    mdsc.insert_run_stop(rs, ttime.time(), uid=str(uuid.uuid4()))

    mds_new = type(mds_all)(mds_all.config)

    ev_gen_base = mdsc.get_events_generator(e_desc)
    ev_gen_reloaded = mds_new.get_events_generator(e_desc)

    for ret, ret_n, expt in zip(ev_gen_base, ev_gen_reloaded, all_data):
        assert ret['descriptor']['uid'] == e_desc
        for k in ['data', 'timestamps', 'time', 'uid', 'seq_num']:
            assert ret[k] == expt[k]
            assert ret_n[k] == expt[k]
