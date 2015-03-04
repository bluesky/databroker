from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import h5py
import tempfile
import uuid

from filestore.api import (insert_resource, insert_datum, retrieve,
                           register_handler, deregister_handler, db_disconnect,
                           db_connect)

import filestore.retrieve as fsr

from filestore.handlers import AreaDetectorHDF5Handler
from filestore.handlers import HDFMapsSpectrumHandler as HDFM
from filestore.handlers import HDFMapsEnergyHandler as HDFE
from numpy.testing import assert_array_equal
import os
from itertools import product
from nose.tools import assert_true, assert_raises

from six.moves import range
db_name = str(uuid.uuid4())
conn = None


def setup():
    global conn
    db_disconnect()
    db_connect(db_name, 'localhost', 27017)

    register_handler('AD_HDF5', AreaDetectorHDF5Handler)


def teardown():
    deregister_handler('AD_HDF5')
    db_disconnect()
    # if we know about a connection, drop the database
    if conn:
        conn.drop_database(db_name)


class _with_file(object):
    # a base-class for testing which provides a temporary file for
    # I/O tests.  This class provides a setup function which creates
    # a temporary file (path stored in `self.filename`).  Sub-classes
    # should over-ride `_make_data` to fill the file with data for the test.
    def setup(self):
        with tempfile.NamedTemporaryFile(delete=False) as fn:
            self.filename = fn.name
        self._make_data()

    def teardown(self):
        os.unlink(self.filename)

    def _make_data(self):
        # sub-classes need to override this to put data into the test file
        pass


class test_AD_hdf5_files(_with_file):
    # test the HDF5 product emitted by the hdf5 plugin to area detector
    def _make_data(self):
        filename = self.filename
        with h5py.File(filename) as f:
            N = 5
            # Write the data.
            data = np.multiply.outer(np.arange(N), np.ones((2, 2)))
            f.create_dataset('/entry/data/data', data=data)

        # Insert the data records.
        resource_id = insert_resource('AD_HDF5', filename)
        self.datum_ids = [str(uuid.uuid4()) for i in range(N)]
        for i, datum_id in enumerate(self.datum_ids):
            insert_datum(resource_id, datum_id, dict(point_number=i))

    def test_AD_round_trip(self):

        # Retrieve the data.
        for i, datum_id in enumerate(self.datum_ids):
            data = retrieve(datum_id)
            known_data = i * np.ones((2, 2))
            assert_array_equal(data, known_data)

    def test_context_manager(self):
        # make sure context manager works
        with AreaDetectorHDF5Handler(self.filename) as hand:
            assert hand._file
            # also test double opening a handler
            hand.open()

    def test_open_close(self):

        hand = AreaDetectorHDF5Handler(self.filename)  # calls open()
        assert_true(hand._file is not None)
        hand.close()
        assert_true(hand._file is None)
        hand.open()
        assert_true(hand._file is not None)
        hand.close()
        assert_true(hand._file is None)


class test_maps_hdf5(_with_file):
    n_pts = 20
    N = 10
    M = 11

    # tests the MAPS handler (product specification from APS)
    def _make_data(self):
        self.th = np.linspace(0, 2*np.pi, self.n_pts)
        self.scale = np.arange(self.N*self.M)

        with h5py.File(self.filename, 'w') as f:
            # create a group for maps to hold the data
            mapsGrp = f.create_group('MAPS')
            # now set a comment
            mapsGrp.attrs['comments'] = 'MAPS group'

            entryname = 'mca_arr'
            comment = 'These are raw spectrum data.'
            sn = np.sin(self.th).reshape(self.n_pts, 1, 1)
            XY = self.scale.reshape(1, self.N, self.M)
            data = XY * sn
            ds_data = mapsGrp.create_dataset(entryname, data=data)
            ds_data.attrs['comments'] = comment

        # insert spectrum-wise resource and datum
        resource_id = insert_resource('hdf5_maps', self.filename,
                                      {'dset_path': 'mca_arr'})
        self.eids_spectrum = [str(uuid.uuid4()) for j in range(self.N*self.M)]

        for uid, (i, j) in zip(self.eids_spectrum,
                               product(range(self.N), range(self.M))):
            insert_datum(resource_id, uid, {'x': i, 'y': j})

        # insert plane-wise resource and datum
        resource_id = insert_resource('hdf5_planes', self.filename,
                                      {'dset_path': 'mca_arr'})
        self.eids_planes = [str(uuid.uuid4()) for j in range(self.n_pts)]

        for uid, n in zip(self.eids_planes, range(self.n_pts)):
            insert_datum(resource_id, uid, {'e_index': n})

    def test_maps_spectrum_round_trip(self):
        sn = np.sin(self.th)

        with fsr.handler_context({'hdf5_maps': HDFM}):
            for eid, sc in zip(self.eids_spectrum, self.scale):
                print(eid)
                data = retrieve(eid)
                assert_array_equal(data, sc * sn)

    def test_maps_plane_round_trip(self):
        base = self.scale.reshape(self.N, self.M)
        with fsr.handler_context({'hdf5_planes': HDFE}):
            for eid, v in zip(self.eids_planes, np.sin(self.th)):
                data = retrieve(eid)
                assert_array_equal(data, base * v)

    def test_closed_rasise(self):
        hand = HDFE(self.filename, 'mca_arr')
        hand.close()
        assert_raises(RuntimeError, hand, 0)
