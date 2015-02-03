import unittest
from ..examples.sample_data import temperature_ramp
from ..broker.struct import BrokerStruct

class TestSeedingData(unittest.TestCase):
    def setUp(self):
        pass

    def test_temperature_ramp(self):
        # smoke test
        events = temperature_ramp.run()

        # check expected types
        self.assertTrue(isinstance(events, list))
        self.assertTrue(isinstance(events[0], BrokerStruct))
