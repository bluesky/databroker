"""Module that defines the Atom Models that back the Search Views"""

import six
from collections import deque
from atom.api import Atom, Typed, List, Range, Dict, observe, Str, Bool, Int
from dataportal.broker import DataBroker
from metadatastore.api import Document
import metadatastore
from mongoengine.connection import ConnectionError
from pymongo.errors import AutoReconnect



class WatchForHeadersModel(Atom):
    """ Class that defines the model for a UI component that watches the
    databroker for new scans

    Attributes
    ----------
    auto_update : atom.Bool
    update_rate : atom.Int
        The rate at which the current header will be checked on the data broker
    header_id : atom.Str
    """
    auto_update = Bool(False)
    update_rate = Int(1000)
    header = Typed(Document)
    search_info = Str("No search performed")

    def check_header(self):
        try:
            header = DataBroker[-1]
        except IndexError:
            self.search_info = "No runs found."
            header = None
            return
        else:
            self.search_info = "Run Found."
        if (not self.header or self.header.ids['run_start_uid'] !=
            header.ids['run_start_uid']):
            self.header = header

class DisplayHeaderModel(Atom):
    """Class that defines the model for displaying header information

    Attributes
    ----------
    selected : metadatastore.api.Document
    """
    header = Typed(Document)
    selected_as_dict = Dict()
    selected_keys = List()

    def new_run_header(self, changed):
        """Observer function for a new run header"""
        self.header = changed['value']

    @observe('header')
    def header_changed(self, changed):
        run_start_keys = {}
        key_labels = [['KEY NAME', 'DATA LOCATION', 'PV NAME']]
        run_start_dict = vars(self.header)
        event_descriptors = run_start_dict.pop('event_descriptors', [])
        data_keys = self._format_for_enaml(event_descriptors)
        sample = run_start_dict.pop('sample', {})
        beamline_config = run_start_dict.pop('beamline_config', {})

        data_keys = sorted(data_keys, key=lambda x: x[0].lower())
        run_start_keys = key_labels + data_keys

        # set the summary dictionary
        self.selected_as_dict = {}
        self.selected_as_dict = run_start_dict
        # set the keys dictionary
        self.selected_keys = []
        self.selected_keys = run_start_keys


    def _format_for_enaml(self, event_descriptors):
        """
        Format the data keys into a single list that enaml will unpack into a
        grid of N rows by 3 columns
        """
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
        return data_keys


class GetLastModel(Atom):
    """Class that defines the model for the 'get last N datasets view'

    Attributes
    ----------
    num_to_retrieve : range, min=1
    headers : list
    selected : metadatastore.api.Document
    """
    num_to_retrieve = Range(low=1)
    search_info = Str()
    headers = List()
    connection_is_active = Bool(False)
    summary_visible = Bool(False)
    header = Typed(Document)

    def __init__(self):
        with self.suppress_notifications():
            self.header = None


    @observe('num_to_retrieve')
    def num_changed(self, changed):
        try:
            self.headers = DataBroker[-self.num_to_retrieve:]
        except ConnectionError:
            self.search_info = "Database [[{}]] not available on [[{}]]".format(
                metadatastore.conf.mds_config['database'],
                metadatastore.conf.mds_config['host']
            )
            self.connection_is_active = False
            return
        except AutoReconnect:
            self.search_info = (
                "Connection to database [[{}]] on [[{}]] was lost".format(
                    metadatastore.conf.mds_config['database'],
                    metadatastore.conf.mds_config['host']
                )
            )
            self.connection_is_active = False
            return
        self.search_info = "Requested: {}. Found: {}".format(
            self.num_to_retrieve, len(self.headers))
