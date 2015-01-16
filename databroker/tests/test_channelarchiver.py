from ..dummy_sources import _channelarchiver as ca
import unittest
from datetime import datetime as dt

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
