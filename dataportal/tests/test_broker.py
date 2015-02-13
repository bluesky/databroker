import unittest
from datetime import datetime
import numpy as np
import pandas as pd
from ..sources import channelarchiver as ca
from ..sources import switch
from ..broker.simple_broker import POPULAR_CHANNELS


class TestBroker(unittest.TestCase):

    def setUp(self):
        switch(channelarchiver=False, metadatastore=False, filestore=False)
        start, end = '2015-01-01 00:00:00', '2015-01-01 00:01:00'
        simulated_ca_data = generate_ca_data(POPULAR_CHANNELS, start, end)
        ca.insert_data(simulated_ca_data)

    def tearDown(self):
        switch(channelarchiver=True, metadatastore=True, filestore=True)


def generate_ca_data(channels, start_time, end_time):
    timestamps = pd.date_range(start_time, end_time, freq='T').to_series()
    timestamps = list(timestamps.dt.to_pydatetime())  # list of datetime objects
    values = list(np.arange(len(timestamps)))
    return {channel: (timestamps, values) for channel in channels}
