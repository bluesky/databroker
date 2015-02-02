import time
import unittest
import numpy as np
from .. import sources
from ..muggler.data import DataMuggler
from ..sources import switch
from ..examples.sample_data import temperature_ramp


class TestMuggler(unittest.TestCase):

    def setUp(self):
        self.mixed_events = temperature_ramp.run()

    def test_empty_muggler(self):
        DataMuggler()

    def test_from_events(self):
        DataMuggler.from_events(self.mixed_events)

    def test_dataframe(self):
        dm = DataMuggler.from_events(self.mixed_events)
        dm._dataframe

    def test_binning(self):
        dm = DataMuggler.from_events(self.mixed_events)
        dm.bin_by_edges([(1, 2), (3, 4)])  # TODO more relavant bins
