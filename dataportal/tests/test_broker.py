import unittest
from datetime import datetime
import numpy as np
import pandas as pd
from ..sources import channelarchiver as ca
from ..sources import switch
from ..examples.sample_data import temperature_ramp
from ..broker import DataBroker as db


class TestBroker(unittest.TestCase):

    def setUp(self):
        switch(channelarchiver=False, metadatastore=True, filestore=True)
        start, end = '2015-01-01 00:00:00', '2015-01-01 00:01:00'
        simulated_ca_data = generate_ca_data(['ch1', 'ch2'], start, end)
        ca.insert_data(simulated_ca_data)
        temperature_ramp.run()

    def test_basic_usage(self):
        header = db[-1]
        events = db.fetch_events(header)

    def tearDown(self):
        switch(channelarchiver=True, metadatastore=True, filestore=True)


def generate_ca_data(channels, start_time, end_time):
    timestamps = pd.date_range(start_time, end_time, freq='T').to_series()
    timestamps = list(timestamps.dt.to_pydatetime())  # list of datetime objects
    values = list(np.arange(len(timestamps)))
    return {channel: (timestamps, values) for channel in channels}
