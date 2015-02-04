from __future__ import print_function
import six
import time
import unittest
import numpy as np
from .. import sources
from ..muggler.data import DataMuggler, BinningError
from ..sources import switch
from ..examples.sample_data import temperature_ramp, multisource_event


class TestMuggler(unittest.TestCase):

    def setUp(self):
        self.mixed_scalars = temperature_ramp.run()

    def test_empty_muggler(self):
        DataMuggler()

    def test_attributes(self):
        dm = DataMuggler.from_events(self.mixed_scalars)
        # merely testing that basic usage does not error
        dm._dataframe
        dm.Tsam
        dm['Tsam']

    def test_timestamps_as_data(self):
        dm = DataMuggler.from_events(self.mixed_scalars)
        dm.include_timestamp_data('Tsam')
        self.assertTrue('Tsam_timestamp' in dm._dataframe)
        dm.remove_timestamp_data('Tsam')
        self.assertFalse('Tsam_timestamp' in dm._dataframe)


class CommonBinningTests(object):

    def test_bin_on_sparse(self):
        "Align a dense column to a sparse column."

        # If downsampling is necessary but there is no rule, fail.
        bad_binning = lambda: self.dm.bin_on(self.sparse)
        self.assertRaises(BinningError, bad_binning)

        result = self.dm.bin_on(self.sparse,
                                agg={self.dense: np.mean})
        # With downsampling, the result should have as many entires as the
        # data source being "binned on" (aligned to).
        actual_len = len(result)
        expected_len = len(self.dm[self.sparse])
        self.assertEqual(actual_len, expected_len)

        # There should be stats columns.
        self.assertTrue('max' in result[self.dense].columns)

    def test_bin_on_dense(self):
        "Align a sparse column to a dense column"

        # With or without upsampling, the result should have the sample
        # length as the dense column.
        expected_len = len(self.dm[self.dense])
        result1 = self.dm.bin_on(self.dense)
        actual_len1 = len(result1)
        self.assertEqual(actual_len1, expected_len)
        result2 = self.dm.bin_on(self.dense,
                                 interpolation={self.sparse: 'linear'})
        actual_len2 = len(result2)
        self.assertEqual(actual_len2, expected_len)

        # If there is an interpolation rule, there should be no missing values
        # except (perhaps) at the edges outside the domain of the sparse col.
        first = self.dm[self.sparse].first_valid_index()
        last = self.dm[self.sparse].last_valid_index()
        expected_len = 2 + len(self.dm[self.dense].loc[first:last])
        self.assertLess(result1[self.sparse]['val'].count(), expected_len)
        self.assertEqual(result2[self.sparse]['val'].count(), expected_len)

        # There should not be stats columns.
        self.assertFalse('max' in result1[self.dense].columns)

        # The dense column, being binned on, should have no 'count' column.
        self.assertFalse('count' in result1[self.dense].columns)
        # But the sparse column should.
        self.assertTrue('count' in result1[self.sparse].columns)


class TestBinningTwoScalarEvents(CommonBinningTests, unittest.TestCase):

    def setUp(self):
        self.dm = DataMuggler.from_events(temperature_ramp.run())
        self.sparse = 'Tsam'
        self.dense = 'point_det'


class TestBinningMultiSourceEvents(CommonBinningTests, unittest.TestCase):

    def setUp(self):
        self.dm = DataMuggler.from_events(multisource_event.run())
        self.sparse = 'Troom'
        self.dense = 'point_det'
