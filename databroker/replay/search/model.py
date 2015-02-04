"""Module that defines the Atom Models that back the Search Views"""

from atom.api import Atom, Typed, List, Range
from databroker.api import BrokerStruct

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
