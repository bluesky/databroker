"""Module that defines the Atom Models that back the Search Views"""

import six
from collections import deque
from atom.api import Atom, Typed, List, Range, Dict, observe, Str, Bool, Int
from dataportal.broker import DataBroker
from metadatastore.api import Document
import metadatastore
from mongoengine.connection import ConnectionError
from pymongo.errors import AutoReconnect
from functools import wraps
from dataportal import replay
from ..persist import History
import logging


logger = logging.getLogger(__name__)


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
    history = Typed(History)

    def __init__(self, history, **kwargs):
        super(WatchForHeadersModel, self).__init__()
        self.history = history
        try:
            state = history.get('WatchForHeadersModel')
        except IndexError:
            # no entries for 'WatchForHeadersModel' yet
            state = {}
        else:
            state.pop('history', None)
        if state:
            self.__setstate__(state)

    @observe('update_rate')
    def save_state(self, changed):
        logger.debug('history in WatchForHeadersModel.save_state: '
                     '{}'.format(self.history))
        replay.core.save_state(self.history, 'WatchForHeadersModel',
                               self.__getstate__())

    def check_header(self):
        try:
            header = DataBroker[-1]
        except IndexError:
            self.search_info = "No runs found."
            header = None
            return
        else:
            self.search_info = "Run Found."
        if (not self.header or self.header.run_start_uid != header.run_start_uid):
            self.header = header


class DisplayHeaderModel(Atom):
    """Class that defines the model for displaying header information

    Attributes
    ----------
    selected : metadatastore.api.Document
    """
    header = Typed(Document)
    header_as_dict = Dict()
    header_keys = List()

    def new_run_header(self, changed):
        """Observer function for a new run header"""
        self.header = changed['value']

    @observe('header')
    def header_changed(self, changed):
        self.header_as_dict = {}
        self.header_keys = []
        if self.header is None:
            return
        key_labels = [['KEY NAME', 'DATA LOCATION', 'PV NAME']]
        header_dict = dict(self.header.items())
        event_descriptors = header_dict.pop('event_descriptors', [])
        data_keys = self._format_for_enaml(event_descriptors)
        sample = header_dict.pop('sample', {})
        beamline_config = header_dict.pop('beamline_config', {})
        # unpack the 'ids' fields
        ids = header_dict.pop('ids', {})
        for id_name, id_val in ids.items():
            header_dict[id_name] = id_val
        data_keys = sorted(data_keys, key=lambda x: x[0].lower())
        header_keys = key_labels + data_keys

        # set the summary dictionary
        self.header_as_dict = header_dict
        # set the keys dictionary
        self.header_keys = header_keys

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
                name = data_key
                src = data_key_dict['source']
                loc = data_key_dict['external']
                if loc is None:
                    loc = 'metadatastore'
                data_keys.append([name, loc, src])
        return data_keys


def _catch_connection_issues(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        try:
            func(self, *args, **kwargs)
        except ConnectionError:
            self.search_info = (
                "Database {} not available at {} on port {}").format(
                    metadatastore.conf.connection_config['database'],
                    metadatastore.conf.connection_config['host'],
                    metadatastore.conf.connection_config['port'])
        except AutoReconnect:
            self.search_info = (
                "Connection to database [[{}]] on [[{}]] was lost".format(
                    metadatastore.conf.connection_config['database'],
                    metadatastore.conf.connection_config['host']))
    return inner


class _BrokerSearch(Atom):
    """ABC for broker searching with Atom classes

    Attributes
    ----------
    headers : atom.List
        The list of headers returned from the DataBroker
    header : metadatastore.api.Document
        The currently selected header
    connection_is_active : atom.Bool
        True: Connection to the DataBroker is active
    search_info : atom.Str
        Potentially informative string that gets displayed on the UI regarding
        the most recently performed DataBroker search
    """
    search_info = Str()
    headers = List()
    header = Typed(Document)
    history = Typed(History)

    def __init__(self):
        with self.suppress_notifications():
            self.header = None


class GetLastModel(_BrokerSearch):
    """Class that defines the model for the 'get last N datasets view'

    Attributes
    ----------
    num_to_retrieve : range, min=1
    """
    num_to_retrieve = Range(low=1)

    def __init__(self, history):
        super(GetLastModel, self).__init__()
        self.header = None
        self.history = history
        try:
            state = history.get('GetLastModel')
        except IndexError:
            # no entries for 'WatchForHeadersModel' yet
            state = {}
        else:
            state.pop('history', None)
        if state:
            self.__setstate__(state)

    @observe('num_to_retrieve')
    @_catch_connection_issues
    def num_changed(self, changed):
        self.headers = DataBroker[-self.num_to_retrieve:]

        self.search_info = "Requested: {}. Found: {}".format(
            self.num_to_retrieve, len(self.headers))
        logger.debug('history in WatchForHeadersModel.save_state: '
                     '{}'.format(self.history))
        replay.core.save_state(self.history, 'GetLastModel',
            {'num_to_retrieve': self.num_to_retrieve})


class ScanIDSearchModel(_BrokerSearch):
    """

    Class that defines the model for a UI component that searches the
    databroker for a specific scan

    Attributes
    ----------
    scan_id : atom.Int
    """
    scan_id = Int(1)

    def __init__(self, history):
        super(ScanIDSearchModel, self).__init__()
        self.header = None
        self.search_info = "Searching by Scan ID"
        self.history = history
        try:
            state = history.get('ScanIDSearchModel')
        except IndexError:
            # no entries for 'WatchForHeadersModel' yet
            state = {}
        else:
            state.pop('history', None)
        if state:
            self.__setstate__(state)

    @observe('scan_id')
    @_catch_connection_issues
    def scan_id_changed(self, changed):
        self.headers = DataBroker.find_headers(scan_id=self.scan_id)
        self.search_info = "Requested scan id: {}. Found: {}".format(
            self.scan_id, len(self.headers))

    @observe('scan_id')
    def save_state(self, changed):
        logger.debug('history in ScanIDSearchModel.save_state: '
                     '{}'.format(self.history))
        replay.core.save_state(self.history, 'ScanIDSearchModel',
            {'scan_id': self.scan_id})
