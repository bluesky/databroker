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
from numpy.testing import assert_array_equal
import os
from itertools import product
from nose.tools import assert_true

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
    def setup(self):
        with tempfile.NamedTemporaryFile(delete=False) as fn:
            self.filename = fn.name
        self._make_data()

    def teardown(self):
        os.unlink(self.filename)


class test_AD_hdf5_files(_with_file):
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
        with AreaDetectorHDF5Handler(self.filename) as hand:
            assert hand._file
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
    def _make_data(self):
        n_pts = 20
        N = 10
        M = 11
        self.th = np.linspace(0, 2*np.pi, n_pts)
        self.scale = np.arange(N*M)
        with h5py.File(self.filename, 'w') as f:
            # create a group for maps to hold the data
            mapsGrp = f.create_group('MAPS')
            # now set a comment

            mapsGrp.attrs['comments'] = 'MAPS group'

            entryname = 'mca_arr'
            comment = 'These are raw spectrum data.'
            sn = np.sin(self.th).reshape(n_pts, 1, 1)
            XY = self.scale.reshape(1, N, M)
            data = XY * sn
            ds_data = mapsGrp.create_dataset(entryname, data=data)
            ds_data.attrs['comments'] = comment

        resource_id = insert_resource('hdf5_maps', self.filename,
                                      {'dset_path': 'mca_arr'})
        self.eids = [str(uuid.uuid4()) for j in range(N*M)]

        for uid, (i, j) in zip(self.eids, product(range(N), range(M))):
            insert_datum(resource_id, uid, {'x': i, 'y': j})

    def test_maps_round_trip(self):
        sn = np.sin(self.th)

        with fsr.handler_context({'hdf5_maps': HDFM}):
            for eid, sc in zip(self.eids, self.scale):
                print(eid)
                data = retrieve(eid)
                assert_array_equal(data, sc * sn)
