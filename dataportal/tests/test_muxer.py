from __future__ import print_function
import six

import uuid


import unittest
import numpy as np

from ..muxer.data_muxer import DataMuxer, BinningError, ColSpec

from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from ..broker import DataBroker
from filestore.utils.testing import fs_setup, fs_teardown
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore.api import insert_run_start

from nose.tools import (assert_true, assert_false)
from numpy.testing.utils import assert_array_equal


def setup():
    fs_setup()
    mds_setup()

    rs_uid = insert_run_start(time=0.0, scan_id=1, owner='test',
                              beamline_id='test',
                              uid=str(uuid.uuid4()))
    temperature_ramp.run(run_start_uid=rs_uid)


def teardown():
    fs_teardown()
    mds_teardown()


def test_empty_muxer():
    DataMuxer()


def test_attributes():
    hdr = DataBroker[-1]
    events = DataBroker.fetch_events(hdr)
    dm = DataMuxer.from_events(events)
    # merely testing that basic usage does not error
    for data_key in dm.sources.keys():
        getattr(dm, data_key)
        dm[data_key]
    properties = ['ncols', '_dataframe', 'col_info_by_ndim', 'sources',
                  'col_info', '_data', '_time', '_timestamps',
                  '_timestamps_as_data', '_known_events', '_known_descriptors',
                  '_stale']
    for prop in properties:
        getattr(dm, prop)
    # dm._dataframe
    # dm.col_info_by_ndim
    # dm.col_info


def test_timestamps_as_data():
    hdr = DataBroker[-1]
    events = DataBroker.fetch_events(hdr)
    dm = DataMuxer.from_events(events)
    data_name = list(dm.sources.keys())
    for name in data_name:
        dm.include_timestamp_data(name)
        assert_true('{}_timestamp'.format(name) in dm._dataframe)
        dm.remove_timestamp_data(name)
        assert_false('{}_timestamp'.format(name) in dm._dataframe)


def run_colspec_programmatically(name, ndim, shape, upsample, downsample):
    ColSpec(name, ndim, shape, upsample, downsample)

def test_colspec():
    defaults = {'name': 'some_col',
                'ndim': 1,
                'shape': (1, 2),
                'upsample': None,
                'downsample': None}
    names = [1, 'name', '', 1.0, range(5), {'a': 5}, [1, 'name']]
    ndims = range(0, 5)
    shapes = [None, tuple([1]), (1, 2), (1, 2, 3)]
    upsamples = ColSpec.upsampling_methods
    downsamples = ColSpec.downsampling_methods
    yield_dict = {'name': names, 'ndim': ndims, 'shape': shapes,
                  'upsample': upsamples, 'downsample': downsamples}
    for key, vals_list in yield_dict.items():
        for val in vals_list:
            yield_dict = defaults.copy()
            yield_dict[key] = val
            yield run_colspec_programmatically, yield_dict['name'], \
                  yield_dict['ndim'], yield_dict['shape'], \
                  yield_dict['upsample'], yield_dict['downsample']

    # # things that will fail
    # ndims = [names[1]] + names[3:]
    # shapes = ndims
    # upsamples = 'cat'
    # downsamples = upsamples
    #
    # for key, vals_list in yield_dict.items():
    #     for val in vals_list:
    #         yield_dict = defaults.copy()
    #         yield_dict[key] = val
    #         yield run_colspec_programmatically, yield_dict['name'], \
    #               yield_dict['ndim'], yield_dict['shape'], \
    #               yield_dict['upsample'], yield_dict['downsample']



class CommonBinningTests(object):

    def test_bin_on_sparse(self):
        "Align a dense column to a sparse column."

        # If downsampling is necessary but there is no rule, fail.
        bad_binning = lambda: self.dm.bin_on(self.sparse)
        self.assertRaises(BinningError, bad_binning)

        self.dm.plan.bin_on(self.sparse, agg={self.dense: self.agg})
        result = self.dm.bin_on(self.sparse,
                                agg={self.dense: self.agg})
        # With downsampling, the result should have as many entires as the
        # data source being "binned on" (aligned to).
        actual_len = len(result)
        expected_len = len(self.dm[self.sparse])
        self.assertEqual(actual_len, expected_len)

        # There should be stats columns.
        self.assertTrue('max' in result[self.dense].columns)

        # There should be one row per bin.
        self.assertTrue(self.dm._dataframe.index.is_unique)
        self.assertTrue(result.index.is_unique)

    def test_bin_on_dense(self):
        "Align a sparse column to a dense column"

        # With or without upsampling, the result should have the sample
        # length as the dense column.
        expected_len = len(self.dm[self.dense])
        result1 = self.dm.bin_on(self.dense)
        actual_len1 = len(result1)
        self.assertEqual(actual_len1, expected_len)
        self.dm.plan.bin_on(self.dense, interpolation={self.sparse: self.interp})
        result2 = self.dm.bin_on(self.dense,
                                 interpolation={self.sparse: self.interp})
        actual_len2 = len(result2)
        self.assertEqual(actual_len2, expected_len)

        # If there is an interpolation rule, there should be no missing values
        # except (perhaps) at the edges outside the domain of the sparse col.
        first, last = result1[self.sparse].dropna().index[[0, -1]]
        self.assertTrue(result2.loc[first:last, self.sparse].notnull().all().all())

        # There should not be stats columns.
        self.assertFalse('max' in result1[self.dense].columns)

        # The dense column, being binned on, should have no 'count' column.
        self.assertFalse('count' in result1[self.dense].columns)
        # But the sparse column should.
        self.assertTrue('count' in result1[self.sparse].columns)

        # There should be one row per bin.
        self.assertTrue(result1.index.is_unique)
        self.assertTrue(result2.index.is_unique)


class TestBinningTwoScalarEvents(CommonBinningTests, unittest.TestCase):

    def setUp(self):
        self.dm = DataMuxer.from_events(temperature_ramp.run())
        self.sparse = 'Tsam'
        self.dense = 'point_det'
        self.agg = np.mean
        self.interp = 'linear'


class TestBinningMultiSourceEvents(CommonBinningTests, unittest.TestCase):

    def setUp(self):
        self.dm = DataMuxer.from_events(multisource_event.run())
        self.sparse = 'Troom'
        self.dense = 'point_det'
        self.agg = np.mean
        self.interp = 'linear'


class TestImageAndScalar(unittest.TestCase):

    def setUp(self):
        self.dm = DataMuxer.from_events(image_and_scalar.run())

    def test_index_is_unique(self):
        self.assertTrue(self.dm._dataframe.index.is_unique)

    def test_bin_on_image(self):
        self.dm.plan.bin_on('img', agg={'Tsam': 'mean'})
        binned = self.dm.bin_on('img', agg={'Tsam': 'mean'})
        self.assertEqual(len(binned), len(self.dm['img']))

    def test_image_shape(self):
        datapoint = self.dm['img'].values[2]
        self.assertEqual(datapoint.ndim, 2)

        self.dm.plan.bin_on('img', agg={'Tsam': 'mean'})
        binned = self.dm.bin_on('img', agg={'Tsam': 'mean'})
        binned_datapoint = binned['img']['val'].values[2]
        self.assertEqual(binned_datapoint.ndim, 2)

        assert_array_equal(datapoint, binned_datapoint)
