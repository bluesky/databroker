import unittest
from ..sources import switch
from ..broker.simple_broker import search


class TestDataBroker(unittest.TestCase):

    def setUp(self):
        switch(channelarchiver=False, metadatastore=False, filestore=False)

    def test_basic_search(self):
        search('srx', '2015-01-01', '2015-02-01')

    def tearDown(self):
        switch(channelarchiver=True, metadatastore=True, filestore=True)
