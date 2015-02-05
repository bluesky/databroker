"""Module that defines the Atom Models that back the Search Views"""

import six
from collections import deque
from atom.api import Atom, Typed, List, Range, Dict, observe, Str
from databroker.api import BrokerStruct
from databroker.broker import simple_broker
from pprint import pprint

class GetLastModel(Atom):
    """Class that defines the model for the 'get last N datasets view'

    Attributes
    ----------
    num_to_retrieve : range, min=1
    begin_run_events : list
    selected : databroker.api.BrokerStruct
    """
    num_to_retrieve = Range(low=1)
    begin_run_events = List()
    selected = Typed(BrokerStruct)
    selected_as_dict = Dict()
    selected_keys = List()
    __begin_run_events_as_dict = Dict()
    __begin_run_events_keys = Dict()

    @observe('selected')
    def selected_changed(self, changed):
        # set the summary dictionary
        self.selected_as_dict = {}
        self.selected_as_dict = self.__begin_run_events_as_dict[self.selected]
        # set the keys dictionary
        print('selected_changed in GetLastModel. self.selected_keys: {}'.format(self.__begin_run_events_keys[self.selected]))
        self.selected_keys = []
        self.selected_keys = self.__begin_run_events_keys[self.selected]

    @observe('num_to_retrieve')
    def num_changed(self, changed):
        self.begin_run_events = simple_broker.get_last_headers(self.num_to_retrieve)
        begin_run_events_as_dict = {}
        begin_run_events_keys = {}
        for bre in self.begin_run_events:
            bre_vars = vars(bre)
            event_descriptors = bre_vars.pop('event_descriptors', [])
            sample = bre_vars.pop('sample', {})
            beamline_config = bre_vars.pop('beamline_config', {})
            dct = bre_vars
            begin_run_events_as_dict[bre] = dct
            data_keys = deque(['KEY NAME', 'PV NAME', 'DATA LOCATION'])
            for evd in event_descriptors:
                dk = evd.data_keys
                for data_key, data_key_dict in six.iteritems(dk):
                    while data_key in data_keys:
                        data_key += '_1'
                    print(data_key, data_key_dict)
                    data_keys.append(data_key)
                    try:
                        data_keys.append(data_key_dict['source'])
                        try:
                            data_keys.append(data_key_dict['EXTERNAL'])
                        except KeyError:
                            data_keys.append('metadatastore')
                    except (KeyError, TypeError):
                        data_keys.append(data_key_dict)
                        data_keys.append('metadatastore')

            begin_run_events_keys[bre] = list(data_keys)

        self.__begin_run_events_as_dict = begin_run_events_as_dict
        self.__begin_run_events_keys = begin_run_events_keys

