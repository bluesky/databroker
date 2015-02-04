"""Module that defines the Atom Models that back the Search Views"""

import six
from collections import OrderedDict
from atom.api import Atom, Typed, List, Range, Dict, observe, Str
from databroker.api import BrokerStruct
from databroker.broker import simple_broker
from pprint import pprint

class LastModel(Atom):
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
    selected_keys = Dict()
    __begin_run_events_as_dict = Dict()
    __begin_run_events_keys = Dict()

    @observe('selected')
    def selected_changed(self, changed):
        # set the summary dictionary
        self.selected_as_dict = {}
        self.selected_as_dict = self.__begin_run_events_as_dict[self.selected]
        # set the keys dictionary
        self.selected_keys = {}
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
            data_keys = {}
            for evd in event_descriptors:
                dk = evd.data_keys
                for k, v in six.iteritems(dk):
                    while k in dct:
                        k += '_1'
                    data_keys[k] = v
            begin_run_events_keys[bre] = data_keys

        self.__begin_run_events_as_dict = begin_run_events_as_dict
        self.__begin_run_events_keys = begin_run_events_keys

