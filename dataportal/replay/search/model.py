"""Module that defines the Atom Models that back the Search Views"""

import six
from collections import deque
from atom.api import Atom, Typed, List, Range, Dict, observe, Str, Bool
from dataportal.broker import simple_broker
from metadataStore.api import Document


class GetLastModel(Atom):
    """Class that defines the model for the 'get last N datasets view'

    Attributes
    ----------
    num_to_retrieve : range, min=1
    headers : list
    selected : metadataStore.api.Document
    """
    num_to_retrieve = Range(low=1)
    headers = List()
    selected = Typed(Document)
    selected_as_dict = Dict()
    selected_keys = List()
    summary_visible = Bool(False)
    __run_starts_as_dict = Dict()
    __run_starts_keys = Dict()

    def __init__(self):
        with self.suppress_notifications():
            self.selected = None


    @observe('selected')
    def selected_changed(self, changed):
        # set the summary dictionary
        self.selected_as_dict = {}
        self.selected_as_dict = self.__run_starts_as_dict[self.selected]
        # set the keys dictionary
        print('selected_changed in GetLastModel. self.selected_keys: {}'.format(self.__run_starts_keys[self.selected]))
        self.selected_keys = []
        self.selected_keys = self.__run_starts_keys[self.selected]

    @observe('num_to_retrieve')
    def num_changed(self, changed):
        self.headers = simple_broker.get_last_headers(self.num_to_retrieve)
        run_starts_as_dict = {}
        run_starts_keys = {}
        header = [['KEY NAME', 'DATA LOCATION', 'PV NAME']]
        for bre in self.headers:
            bre_vars = vars(bre)
            event_descriptors = bre_vars.pop('event_descriptors', [])
            sample = bre_vars.pop('sample', {})
            beamline_config = bre_vars.pop('beamline_config', {})
            dct = bre_vars
            run_starts_as_dict[bre] = dct
            # format the data keys into a single list that enaml will unpack
            # into a N rows by 3 columns grid
            data_keys = []
            for evd in event_descriptors:
                dk = evd.data_keys
                for data_key, data_key_dict in six.iteritems(dk):
                    while data_key in data_keys:
                        data_key += '_1'
                    print(data_key, data_key_dict)
                    name = data_key
                    src = data_key_dict['source']
                    loc = data_key_dict['external']
                    if loc is None:
                        loc = 'metadatastore'
                    data_keys.append([name, loc, src])
            data_keys = sorted(data_keys, key=lambda x: x[0].lower())
            run_starts_keys[bre] = header + data_keys
        self.__run_starts_as_dict = run_starts_as_dict
        self.__run_starts_keys = run_starts_keys
