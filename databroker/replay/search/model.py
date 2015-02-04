"""Module that defines the Atom Models that back the Search Views"""

from atom.api import Atom, Typed, List, Range, Dict, observe
from databroker.api import BrokerStruct
from databroker.broker import simple_broker

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

    @observe('selected')
    def selected_changed(self, changed):
        self.selected_as_dict = {'id': 'None'}
        try:
            self.selected_as_dict = vars(self.selected)
        except TypeError:
            # do nothing, because selected_as_dict was already set to an
            # empty dictionary at the top of this function
            pass

    @observe('num_to_retrieve')
    def num_changed(self, changed):
        self.begin_run_events = simple_broker.get_last_headers(self.num_to_retrieve)
