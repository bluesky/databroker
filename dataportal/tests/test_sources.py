from ..sources.dummy_sources import _channelarchiver as ca
from ..sources.dummy_sources import _metadataStore as ds
from ..sources.dummy_sources import _fileStore as fs
from .. import sources
import unittest
from datetime import datetime as dt  # noqa
import numpy as np
from numpy.testing import assert_array_equal


class TestSources(unittest.TestCase):

    def setUp(self):
        pass

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

    def test_switch_metadatastore(self):
        sources.switch(metadatastore=False)
        is_dummy = sources.metadataStore.__file__.count('dummy') == 1
        self.assertTrue(is_dummy)
        sources.switch(metadatastore=True)
        is_dummy = sources.metadataStore.__file__.count('dummy') == 1
        self.assertFalse(is_dummy)

    def test_switch_filestore(self):
        sources.switch(filestore=False)
        is_dummy = sources.fileStore.__file__.count('dummy') == 1
        self.assertTrue(is_dummy)
        sources.switch(filestore=True)
        is_dummy = sources.fileStore.__file__.count('dummy') == 1
        self.assertFalse(is_dummy)

    def tearDown(self):
        sources.switch(channelarchiver=True)
        sources.switch(metadatastore=True)
        sources.switch(filestore=True)


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


class TestFileStore(unittest.TestCase):

    def setUp(self):
        pass

    def retrieve_simulated_data(self):
        expected = np.arange(10)
        value = fs.commands.retrieve_data('np.arange(10)')
        assert_array_equal(value, expected)
        value = fs.commands.retrieve_data('numpy.arange(10)')
        assert_array_equal(value, expected)
