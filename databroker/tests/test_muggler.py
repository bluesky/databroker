import time
import unittest
import numpy as np
from .. import sources
from ..muggler.data import DataMuggler
from ..sources import switch


class TestMuggler(unittest.TestCase):

    def setUp(self):
        switch(metadatastore=True)
        save_begin_run = sources.metadataStore.api.collection.save_begin_run
        save_event_descriptor = \
            sources.metadataStore.api.collection.save_event_descriptor
        save_event = sources.metadataStore.api.collection.save_event

        data_keys = {'linear_motor': {'source': 'PV1'},
                     'scalar_detector': {'source': 'PV2'},
                     'Tsam': {'source': 'PV3'}}

        bre = save_begin_run(time.time(), 'csx')

        ev_desc = save_event_descriptor(bre, data_keys=data_keys)

        func = np.cos
        num = 100
        start = 0
        stop = 10
        sleep_time = .05

        events = []
        for idx, i in enumerate(np.linspace(start, stop, num)):
            data = {'linear_motor': i,
                    'Tsam': i + 5,
                    'scalar_detector': func(i)}
            event = save_event(ev_desc, time.time() + i, data)
            events.append(event)
        time.sleep(sleep_time)

        self.events = events
        self.ev_desc = ev_desc
        self.bre = bre
        self.data_keys = data_keys

    def test_empty_muggler(self):
        DataMuggler()

    def test_from_events(self):
        DataMuggler.from_events(self.events)

    def test_dataframe(self):
        dm = DataMuggler.from_events(self.events)
        dm._dataframe

    def test_binning(self):
        dm = DataMuggler.from_events(self.events)
        dm.bin_by_edges([(1, 2), (3, 4)])  # TODO more relavant bins

    def test_attributes(self):
        dm = DataMuggler.from_events(self.events)
        sources = dm.sources
        expected_sources = {k: v['source'] for k, v in self.data_keys.items()}
        self.assertEqual(sources, expected_sources)
