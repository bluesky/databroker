import numpy as np
from nose.tools import assert_equal, assert_not_equal
from filestore.api import retrieve
from ..examples.hdf_io import hdf_data_io
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.test.utils import fs_setup, fs_teardown


def setup_module():
    fs_setup()
    mds_setup()


def teardown_module():
    fs_teardown()
    mds_teardown()


def _test_retrieve_data(event):
    uid = event['data']['xrf_spectrum']
    data = retrieve(uid)
    assert_equal(data.size, 20)
    assert_not_equal(np.sum(data), 0)


def test_hdf_io():
    events = hdf_data_io()
    for e in events:
        _test_retrieve_data(e)
