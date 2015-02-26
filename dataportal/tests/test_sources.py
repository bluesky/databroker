from ..sources.dummy_sources import _channelarchiver as ca
from ..sources.dummy_sources import _metadatastore as ds
from ..sources.dummy_sources import _filestore as fs
from .. import sources
import unittest
from datetime import datetime as dt  # noqa
import numpy as np
from numpy.testing import assert_array_equal


class TestSources(unittest.TestCase):

    def setUp(self):
        is_dummy = sources.channelarchiver.__file__.count('dummy') == 1
        self._original_state = not is_dummy

    def test_switch_usage(self):
        sources.switch(channelarchiver=True)
        sources.switch(channelarchiver=False)
        sources.switch(channelarchiver=True)

    def test_switch_channelarchiver(self):
        sources.switch(channelarchiver=False)
        is_dummy = sources.channelarchiver.__file__.count('dummy') == 1
        self.assertTrue(is_dummy)
        sources.switch(channelarchiver=True)
        is_dummy = sources.channelarchiver.__file__.count('dummy') == 1
        self.assertFalse(is_dummy)

    def tearDown(self):
        sources.switch(channelarchiver=self._original_state)


class TestArchiver(unittest.TestCase):

    def setUp(self):
        pass

    def pass_through_test(self):
        data = {'channel1': ([dt(2014, 1, 1), dt(2014, 1, 2)], [1, 2])}
        ca.insert_data(data)
        times, values = data['channel1']
        archiver = ca.Archiver('host')
        result = archiver.get('channel1', '2014-01-01', '2014-01-02',
                              interpolation='raw')
        # Get attributes outside of assert so if they are missing
        # we get an error instead of a fail.
        result_values = result.values
        result_times = result.times
        self.assertEqual(values, result_values)
        self.assertEqual(times, result_times)
