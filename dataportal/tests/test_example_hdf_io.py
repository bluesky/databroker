import numpy as np
from nose.tools import assert_equal, assert_not_equal
import filestore.commands as fsc
from ..examples.hdf_io import hdf_data_io


def _test_retrieve_data(evt):
    data = fsc.retrieve_data(evt['data']['xrf_spectrum'][0])
    assert_equal(data.size, 20)
    assert_not_equal(np.sum(data), 0)


def test_hdf_io():
    events = hdf_data_io()
    for e in events:
        yield _test_retrieve_data, e

