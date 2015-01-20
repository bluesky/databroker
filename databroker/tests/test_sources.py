from ..sources.dummy_sources import _channelarchiver as ca
from .. import sources
import unittest
from datetime import datetime as dt  # noqa


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

    def tearDown(self):
        sources.switch(channelarchiver=True)
        sources.switch(metadatastore=True)


class TestArchiver(unittest.TestCase):

    def setUp(self):
        pass

    def pass_through_test(self):
        instructions = '([1,2], [dt(2014, 01, 01), dt(2014, 01, 02)])'
        values, times = eval(instructions)
        archiver = ca.Archiver('dummy host')
        result = archiver.get(instructions, '2014-01-01', '2014-01-02',
                              interpolation='raw')
        # Get attributes outside of assert so if they are missing
        # we get an error instead of a fail.
        result_values = result.values
        result_times = result.times
        self.assertEqual(values, result_values)
        self.assertEqual(times, result_times)
