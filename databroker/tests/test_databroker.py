import unittest
from ..sources import switch
from ..api.databroker import DataBroker

class TestDataBroker(unittest.TestCase):

    def setUp(self):
        switch(metadatastore=False)

    def test_instantiation(self):
        # codename works
        DataBroker('23id')
        DataBroker('23ID')

        # alias works
        DataBroker('csx')
        DataBroker('CSX')
        DataBroker('CsX')

        # gibberish fails
        self.assertRaises(ValueError, lambda: DataBroker('zzzzzz'))

        # nonstring fails
        self.assertRaises(NotImplementedError, lambda: DataBroker(1))

    def test_basic_search(self):
        broker = DataBroker('srx')
        broker.search('2015-01-01', '2015-02-01')

    def tearDown(self):
        switch(metadatastore=True)

    
