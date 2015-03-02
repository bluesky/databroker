from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import h5py
import tempfile
import uuid
import mongoengine
import mongoengine.connection
from filestore.api import (insert_resource, insert_datum, retrieve,
                           register_handler, deregister_handler, db_disconnect,
                           db_connect)
from filestore.handlers import AreaDetectorHDF5Handler
from numpy.testing import assert_array_equal
import os

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
