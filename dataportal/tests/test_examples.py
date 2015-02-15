import unittest
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from metadataStore.api import Document

class CommonSampleDataTests(object):
    def setUp(self):
        pass

    def test_basic_usage(self):
        events = self.example.run()

        # check expected types
        self.assertTrue(isinstance(events, list))
        self.assertTrue(isinstance(events[0], Document))


class TestTemperatureRamp(CommonSampleDataTests, unittest.TestCase):

    def setUp(self):
        self.example = temperature_ramp


class TestMultisourceEvent(CommonSampleDataTests, unittest.TestCase):

    def setUp(self):
        self.example = multisource_event


class TestImageAndScalar(CommonSampleDataTests, unittest.TestCase):

    def setUp(self):
        self.example = image_and_scalar
