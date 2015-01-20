import unittest
from ..api.databroker import DataBroker

class TestDataBroker(unittest.TestCase):

    def setUp(self):
        pass

    def test_codename_works(self):
        DataBroker('23id')
        DataBroker('23ID')

    def test_alias_works(self):
        DataBroker('csx')
        DataBroker('CSX')
        DataBroker('CsX')

    def test_gibberish_fails(self):
        self.assertRaises(ValueError, lambda: DataBroker('zzzzzz'))

    def test_nonstring_fails(self):
        self.assertRaises(NotImplementedError, lambda: DataBroker(1))
