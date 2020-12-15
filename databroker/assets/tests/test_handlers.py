from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import unittest
import six
import numpy as np
import h5py
import tempfile
import time
import tifffile
import uuid

from ..handlers import AreaDetectorHDF5Handler
from ..handlers import AreaDetectorHDF5SWMRHandler
from ..handlers import AreaDetectorHDF5TimestampHandler
from ..handlers import AreaDetectorHDF5SWMRTimestampHandler
from ..handlers import AreaDetectorTiffHandler
from ..handlers import DummyAreaDetectorHandler
from ..handlers import HDFMapsSpectrumHandler as HDFM
from ..handlers import HDFMapsEnergyHandler as HDFE
from ..handlers import NpyFrameWise
from ..path_only_handlers import (AreaDetectorTiffPathOnlyHandler,
                                  RawHandler)
from numpy.testing import assert_array_equal
import os
import shutil
from itertools import product
import pytest
import itertools
from six.moves import range


@pytest.fixture(scope='class')
def fs_with_handlers(request, fs_cls):
    fs = fs_cls
    print('IN FIXTURE')
    fs.register_handler('AD_HDF5', AreaDetectorHDF5Handler)
    fs.register_handler('AD_HDF5_SWMR', AreaDetectorHDF5SWMRHandler)
    fs.register_handler('AD_HDF5_TS', AreaDetectorHDF5TimestampHandler)
    fs.register_handler('AD_HDF5_SWMR_TS',
                        AreaDetectorHDF5SWMRTimestampHandler)

    def deregister_handlers():
        fs.deregister_handler('AD_HDF5')
        fs.deregister_handler('AD_HDF5_SWMR')
        fs.deregister_handler('AD_HDF5_TS')
        fs.deregister_handler('AD_HDF5_SWMR_TS')

    request.addfinalizer(deregister_handlers)
    request.cls.fs = fs
    request.cls._setup_class()
    return fs


@pytest.mark.usefixtures('fs_with_handlers')
class _with_file(object):
    # a base-class for testing which provides a temporary file for
    # I/O tests.  This class provides a setup function which creates
    # a temporary file (path stored in `self.filename`).  Sub-classes
    # should over-ride `_make_data` to fill the file with data for the test.
    @classmethod
    def _setup_class(self):
        with tempfile.NamedTemporaryFile(delete=False) as fn:
            self.filename = fn.name
        # This is flaky. Put it in a retry loop.
        RETRIES = 10
        WAIT_BETWEEN_ATTEMPTS = 2  # seconds
        exc = None
        for attempt_ in range(RETRIES):
            try:
                self._make_data()
            except TimeoutError as exc_:
                exc = exc_
                # Sleep, then retry.
                time.sleep(WAIT_BETWEEN_ATTEMPTS)
                continue
            else:
                break
        else:
            # We have exhausted the retries.
            raise exc

    @classmethod
    def teardown_class(self):
        os.unlink(self.filename)

    @classmethod
    def _make_data(self, fs):
        # sub-classes need to override this to put data into the test file
        pass


@pytest.mark.usefixtures('fs_with_handlers')
class _with_path(object):
    # a base-class for testing which provides a temporary directory for
    # I/O tests.  This class provides a setup function which creates
    # a temporary directory (path stored in `self.filepath`).  Sub-classes
    # should over-ride `_make_data` to fill the file with data for the test.
    @classmethod
    def _setup_class(self):
        self.filepath = tempfile.mkdtemp() + '/'
        self._make_data()

    @classmethod
    def teardown_class(self):
        shutil.rmtree(self.filepath)

    @classmethod
    def _make_data(self, fs):
        # sub-classes need to override this to put data into the test file
        pass


class Test_np_FW(_with_file):
    @classmethod
    def _make_data(self):
        N = 15
        filename = self.filename
        data = np.ones((N, 9, 8)) * np.arange(N).reshape(N, 1, 1)
        np.save(filename, data)
        # Insert the data records.
        resource_id = self.fs.insert_resource(
            'npy_FRAMEWISE', filename + '.npy', {})
        self.datum_ids = [str(uuid.uuid4()) for i in range(N)]
        for i, datum_id in enumerate(self.datum_ids):
            self.fs.insert_datum(resource_id, datum_id, dict(frame_no=i))

    def test_retrieval(self):
        with self.fs.handler_context({'npy_FRAMEWISE': NpyFrameWise}):
            for i, datum_id in enumerate(self.datum_ids):
                data = self.fs.retrieve(datum_id)
                known_data = i * np.ones((9, 8))
                assert_array_equal(data, known_data)


class Test_AD_hdf5_files(_with_file):
    # test the HDF5 product emitted by the hdf5 plugin to area detector

    spec = 'AD_HDF5'
    handler = AreaDetectorHDF5Handler

    @classmethod
    def _make_data(self):
        filename = self.filename
        with h5py.File(filename, 'w') as f:
            N = 5
            # Write the data.
            data = np.multiply.outer(np.arange(N), np.ones((2, 2)))
            f.create_dataset('/entry/data/data', data=data)

        # Insert the data records.
        resource_id = self.fs.insert_resource(self.spec, filename, {})
        self.datum_ids = [str(uuid.uuid4()) for i in range(N)]
        for i, datum_id in enumerate(self.datum_ids):
            self.fs.insert_datum(resource_id, datum_id, dict(point_number=i))

    def test_AD_round_trip(self):

        # Retrieve the data.
        for i, datum_id in enumerate(self.datum_ids):
            data = self.fs.retrieve(datum_id)
            known_data = i * np.ones((1, 2, 2))
            assert_array_equal(data, known_data)

    def test_context_manager(self):
        # make sure context manager works
        with self.handler(self.filename) as hand:
            assert hand._file
            # also test double opening a handler
            hand.open()

    def test_open_close(self):

        hand = self.handler(self.filename)  # calls open()
        assert hand._file is not None
        hand.close()
        assert hand._file is None
        hand.open()
        assert hand._file is not None
        hand.close()
        assert hand._file is None


# class Test_AD_hdf5_SWMR_files(Test_AD_hdf5_files):
#     # test the HDF5 product emitted by the hdf5 plugin to area detector
#
#     spec = 'AD_HDF5_SWMR'
#     handler = AreaDetectorHDF5SWMRHandler
#
#
class Test_AD_hdf5_timestamp_files(_with_file):
    # test the HDF5 product emitted by the hdf5 plugin to area detector
    # This is for the timestamp field

    spec = 'AD_HDF5_TS'
    handler = AreaDetectorHDF5TimestampHandler

    @classmethod
    def _make_data(self):
        filename = self.filename
        with h5py.File(filename, 'w') as f:
            N = 5
            # Write the data.
            data = np.arange(N, dtype=np.float64)
            f.create_dataset(
                '/entry/instrument/NDAttributes/NDArrayEpicsTSSec',
                data=data)
            f.create_dataset(
                '/entry/instrument/NDAttributes/NDArrayEpicsTSnSec',
                data=data * 1e9)

        # Insert the data records.
        resource_id = self.fs.insert_resource(self.spec, filename, {})
        self.datum_ids = [str(uuid.uuid4()) for i in range(N)]
        for i, datum_id in enumerate(self.datum_ids):
            self.fs.insert_datum(resource_id, datum_id, dict(point_number=i))

    def test_AD_round_trip(self):

        # Retrieve the data.
        for i, datum_id in zip(np.arange(len(self.datum_ids)), self.datum_ids):
            data = self.fs.retrieve(datum_id)
            known_data = 2 * i
            assert_array_equal(data, known_data)

    def test_context_manager(self):
        # make sure context manager works
        with self.handler(self.filename) as hand:
            assert hand._file
            # also test double opening a handler
            hand.open()

    def test_open_close(self):

        hand = self.handler(self.filename)  # calls open()
        assert hand._file is not None
        hand.close()
        assert hand._file is None
        hand.open()
        assert hand._file is not None
        hand.close()
        assert hand._file is None


# class Test_AD_hdf5_SWMR_timestamp_files(Test_AD_hdf5_timestamp_files):
#    # test the HDF5 product emitted by the hdf5 plugin to area detector
#
#    spec = 'AD_HDF5_SWMR_TS'
#    handler = AreaDetectorHDF5SWMRTimestampHandler
#
#
class Test_maps_hdf5(_with_file):
    n_pts = 20
    N = 10
    M = 11

    # tests the MAPS handler (product specification from APS)
    @classmethod
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
        resource_id = self.fs.insert_resource('hdf5_maps', self.filename,
                                              {'dset_path': 'mca_arr'})
        self.datum_ids_spectrum = [str(uuid.uuid4()) for j in range(self.N*self.M)]

        for uid, (i, j) in zip(self.datum_ids_spectrum,
                               product(range(self.N), range(self.M))):
            self.fs.insert_datum(resource_id, uid, {'x': i, 'y': j})

        # insert plane-wise resource and datum
        resource_id = self.fs.insert_resource('hdf5_planes', self.filename,
                                              {'dset_path': 'mca_arr'})
        self.datum_ids_planes = [str(uuid.uuid4()) for j in range(self.n_pts)]

        for uid, n in zip(self.datum_ids_planes, range(self.n_pts)):
            self.fs.insert_datum(resource_id, uid, {'e_index': n})

    def test_maps_spectrum_round_trip(self):
        sn = np.sin(self.th)

        with self.fs.handler_context({'hdf5_maps': HDFM}):
            for datum_id, sc in zip(self.datum_ids_spectrum, self.scale):
                print(datum_id)
                data = self.fs.retrieve(datum_id)
                assert_array_equal(data, sc * sn)

    def test_maps_plane_round_trip(self):
        base = self.scale.reshape(self.N, self.M)
        with self.fs.handler_context({'hdf5_planes': HDFE}):
            for datum_id, v in zip(self.datum_ids_planes, np.sin(self.th)):
                data = self.fs.retrieve(datum_id)
                assert_array_equal(data, base * v)

    def test_closed_raise(self):
        hand = HDFE(self.filename, 'mca_arr')
        hand.close()
        with pytest.raises(RuntimeError):
            hand(0)


class Test_ADTiff_files(_with_path):
    template = '%s%s_%05d.tiff'
    fpp = 2
    n_frames = 10
    fname = 'FS_TESTING'
    fr_shape = (10, 15)

    @classmethod
    def _make_data(self):
        self.fn_list = []
        for j in range(self.n_frames * self.fpp):
            fn = self.template % (self.filepath, self.fname, j)
            tifffile.imsave(fn, np.ones(self.fr_shape) * j)
            self.fn_list.append(fn)

    def test_read(self):
        hand = AreaDetectorTiffHandler(self.filepath, self.template,
                                       self.fname, self.fpp)
        abs_count = 0
        for j in range(self.n_frames):
            ret = hand(j)
            assert (self.fpp,) + self.fr_shape == ret.shape
            for fr in ret:
                assert np.all(fr == abs_count)
                abs_count += 1

    def test_filename_list(self):
        inp = sorted(self.fn_list)
        hand = AreaDetectorTiffHandler(self.filepath, self.template,
                                       self.fname, self.fpp)
        datum_kwarg_gen = ({'point_number': j} for j in range(self.n_frames))
        fn_list = sorted(hand.get_file_list(datum_kwarg_gen))

        assert fn_list == inp


@pytest.mark.parametrize('npts, kw', itertools.product((1, 5), ({}, {'a': 1})))
def test_ADDummy(npts, kw):
    hand = DummyAreaDetectorHandler(None, frame_per_point=npts, aadvark=5)
    target_data = (np.ones((npts, 10, 10)) * np.nan).squeeze()
    assert_array_equal(target_data, hand(**kw))


def test_npyfw_fail():
    with pytest.raises(IOError):
        NpyFrameWise('aarvark_rises')


@pytest.mark.parametrize('path, fname, fpp',
                         itertools.product(['/foo/'], ['baz'], (1, 5)))
def test_tiff_path_only(path, fname, fpp):
    test = AreaDetectorTiffPathOnlyHandler(path, '%s%s_%6.6d.tiff', fname, fpp)
    template = '{}{}_{{:06d}}.tiff'.format(path, fname)
    for j in range(5):
        res = test(j)
        expected = [template.format(n) for n in range(j*fpp, (j+1)*fpp)]
        assert res == expected


def test_raw_handler():
    h = RawHandler('path', a=1)
    result = h(b=2)
    assert result == ('path', {'a': 1}, {'b': 2})
