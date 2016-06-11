from ..examples.hdf_io import hdf_data_io
from metadatastore.test.utils import mds_setup, mds_teardown
from filestore.test.utils import fs_setup, fs_teardown
from databroker import DataBroker as db, get_events
from numpy.testing.utils import assert_array_equal


def setup_module(module):
    fs_setup()
    mds_setup()


def teardown_module(module):
    fs_teardown()
    mds_teardown()


def _retrieve_data_helper(event, cols):
    spec = event['data']['xrf_spectrum']
    x = event['data']['h_pos']
    y = event['data']['v_pos']
    assert spec.size == 20
    assert_array_equal(spec, y * cols + x)


def test_hdf_io():
    rows, cols = 1, 5
    rs_uid, ev_uids = hdf_data_io(rows, cols)
    h = db[rs_uid]
    for e in get_events(h, fill=True):
        _retrieve_data_helper(e, cols)
        assert e['uid'] in ev_uids
