from __future__ import print_function
import six
import time
import unittest
import numpy as np
from .. import sources
from ..muggler.data import DataMuggler, BinningError
from ..sources import switch
from ..examples.sample_data import temperature_ramp


class TestMuggler(unittest.TestCase):

    def setUp(self):
        self.mixed_scalars = temperature_ramp.run()
        self.dm = DataMuggler.from_events(self.mixed_scalars)

    def test_empty_muggler(self):
        DataMuggler()

    def test_attributes(self):
        dm = DataMuggler.from_events(self.mixed_scalars)
        # merely testing that basic usage does not error
        dm._dataframe
        dm.Tsam
        dm['Tsam']

    def test_bin_on(self):
        sparse_col_name = 'Tsam'
        dense_col_name = 'point_det'

        # ALIGNING A DENSE COLUMN TO A SPARSE COLUMN

        # If downsampling is necessary but there is no rule, fail.
        bad_binning = lambda: self.dm.bin_on(sparse_col_name)
        self.assertRaises(BinningError, bad_binning)

        # With downsampling, the result should have as many entires as the
        # data source being "binned on" (aligned to).
        actual_len = len(self.dm.bin_on(sparse_col_name,
                                        agg={dense_col_name: np.mean}))
        expected_len = len(self.dm[sparse_col_name])
        self.assertEqual(actual_len, expected_len)

        # ALIGNING A SPARSE COLUMNS TO A DENSE COLUMN

        # With or without upsampling, the result should have the sample
        # length as the dense column.
        expected_len = len(self.dm[dense_col_name])
        result1 = self.dm.bin_on(dense_col_name)
        actual_len1 = len(result1)
        self.assertEqual(actual_len1, expected_len)
        result2 = self.dm.bin_on(dense_col_name,
                                 interpolation={sparse_col_name: 'linear'})
        actual_len2 = len(result2)
        self.assertEqual(actual_len2, expected_len)

        # If there is an interpolation rule, there should be no missing values
        # except (perhaps) at the edges outside the domain of the sparse col.
        first = self.dm[sparse_col_name].first_valid_index()
        last = self.dm[sparse_col_name].last_valid_index()
        expected_len = 2 + len(self.dm[dense_col_name].loc[first:last])
        self.assertLess(result1[sparse_col_name].count(), expected_len)
        self.assertEqual(result2[sparse_col_name].count(), expected_len)
