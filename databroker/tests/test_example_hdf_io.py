
from databroker import DataBroker as db, get_events

from databroker.examples.hdf_io import hdf_data_io
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
from numpy.testing.utils import assert_array_equal


def setup():
    fs_setup()
    mds_setup()


def teardown():
    fs_teardown()
    mds_teardown()


def _test_retrieve_data(event, rows, cols):
    spec = event['data']['xrf_spectrum']
    x = event['data']['h_pos']
    y = event['data']['v_pos']
    assert spec.size == 20
    assert_array_equal(spec, y * cols + x)


def test_hdf_io():
    rows, cols = 1, 5
    rs_uid, ev_uids = hdf_data_io(rows, cols)
    h = db[rs_uid]
    for e in get_events(h):
        yield _test_retrieve_data, e, rows, cols
        assert e['uid'] in ev_uids
