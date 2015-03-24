"""Module that defines the all Models related to DataMuxer manipulations"""

import six
from collections import deque
from atom.api import (Atom, Typed, List, Range, Dict, observe, Str, Enum, Int,
                      Bool, ReadOnly, Tuple, Float)
from dataportal.muxer.api import DataMuxer
from dataportal.broker import DataBroker, EventQueue
from dataportal.muxer.data_muxer import ColSpec
from metadatastore.api import Document
from pandas import DataFrame
import time as ttime
import logging


logger = logging.getLogger(__name__)


def get_events(run_header):
    return DataBroker.fetch_events(run_header)


class ColumnModel(Atom):
    """Atom implementation of dataportal.muxer.data.ColSpec
    """
    name = Str()
    dim = Int()
    data_muxer = Typed(DataMuxer)
    upsample = Enum(*ColSpec.upsampling_methods)
    downsample = Enum(*ColSpec.downsampling_methods)
    _shape = Tuple()
    is_being_normalized = Bool(False)
    can_be_normalized = Bool(True)

    def __init__(self, data_muxer, dim, name, shape, upsample=None,
                 downsample=None):
        self.name = name
        self.dim = dim
        self.data_muxer = data_muxer
        self.shape = shape
        if upsample is None:
            upsample = 'None'
        if downsample is None:
            downsample = 'None'
        self.upsample = upsample
        self.downsample = downsample

    @observe('upsample', 'downsample')
    def sampling_changed(self, changed):
        logger.debug('Old data_muxer col_info: %s', self.data_muxer.col_info[self.name])
        # upsample = self.upsample
        # if upsample == 'None':
        #     upsample = None
        # downsample = self.downsample
        # if downsample == 'None':
        #     downsample = None
        # replace the column info on the data_muxer with the new upsample
        # or downsample
        self.data_muxer.col_info[self.name] = ColSpec(
            self.name, self.dim, self.shape, self.upsample, self.downsample)
        logger.debug('New data_muxer col_info: %s', self.data_muxer.col_info[self.name])

    def __repr__(self):
        return ('ColumnModel(name={}, data_muxer={}, dim={}, upsample={}, '
                'downsample={})'.format(self.name, self.data_muxer, self.dim,
                                        self.upsample, self.downsample))
    @property
    def shape(self):
        return self._shape
    @shape.setter
    def shape(self, value):
        if value is None:
            value = tuple()
        self._shape = value

class MuxerModel(Atom):
    """Class that defines the Model for the data muxer

    Attributes
    ----------
    data_muxer : dataportal.muxer.api.DataMuxer
        The data_muxer holds the non-time-aligned data.  Upon asking the data_muxer
        to reformat its data into time-aligned bins, a dataframe is returned
    run_header: metadatastore.api.Document
        The bucket of information from the data broker that contains all
        non-data information

    column_models : atom.dict.Dict
        Dictionary that is analogous to the col_info property of the
        dataportal.muxer.data.DataMuxer object
    scalar_columns : atom.list.List
        The list of columns names whose cells contain 0-D arrays (single values)
    line_columns : atom.list.List
        The list of column names whose cells contain 1-D arrays
    image_columns : atom.list.List
        The list of column names whose cells contain 2-D arrays
    volume_columns : atom.list.List
        The list of column names whos cells contain 3-D arrays

    scalar_columns_visible : atom.scalars.Bool
        Instructs the GUI to show/hide the scalar info
    line_columns_visible : atom.scalars.Bool
        Instructs the GUI to show/hide the line info
    image_columns_visible : atom.scalars.Bool
        Instructs the GUI to show/hide the image info
    volume_columns_visible : atom.scalars.Bool
        Instructs the GUI to show/hide the volume info

    info : atom.scalars.Str
        A short string describing the `data_muxer` attribute of the Atom
        MuxerModel

    new_data_callbacks : atom.list.List
        List of callbacks that care when the data_muxer gets new data.
        Callback functions should expect no information to be passed.

    auto_updating : atom.Bool
        Is the databroker going to be regularly asked for data?
        True -> yes. False -> no

    update_rate : atom.Int
        The rate at which the databroker will be asked for new data

    binning_options : atom.List
        The column names that can be used to bin on
    binning_axis : atom.Str
        The name of the data stream to use as the binning axis

    norm_options : atom.List
        The column names that can be used as normalization target
    norm_axis : atom.Str
        The name of the data stream to use as the binning axis

    upsample : atom.Enum
        An enum which details the valid options for upsampling the
        data in the data muxer.  This is obtained from
        dataportal.muxer.ColSpec.upsampling_methods and defaults to 'linear'
    downsample : atom.Enum
        An enum which details the valid options for downsampling the
        data in the data muxer.  This is obtained from
        dataportal.muxer.ColSpec.downsampling_methods and defaults to 'mean'

    plot_state : atom.Dict
        The state that should be passed along to any/all plotting widgets.
        This should be hooked up via the 'observe' top level atom function.
        e.g., instance_of_muxer_model.observe('plot_state', target_function)

    """
    column_models = Dict()
    scalar_columns = List(item=ColumnModel)
    line_columns = List(item=ColumnModel)
    image_columns = List(item=ColumnModel)
    volume_columns = List(item=ColumnModel)

    scalar_columns_visible = Bool(False)
    line_columns_visible = Bool(False)
    image_columns_visible = Bool(False)
    volume_columns_visible = Bool(False)

    event_queue = Typed(EventQueue)
    data_muxer = Typed(DataMuxer)
    dataframe = Typed(DataFrame)
    header = Typed(Document)
    header_id = Str()
    info = Str()

    new_data_callbacks = List()

    auto_updating = Bool(False)

    update_rate = Int(2000) # in ms

    binning_options = List(default=['None'])
    binning_column = Str('None')
    binning_index = Int()

    norm_options = List(default=['None'])
    norm_column = Str('None')
    norm_index = Int()

    upsample = Enum('linear', *ColSpec.upsampling_methods)
    downsample = Enum('mean', *ColSpec.downsampling_methods)

    plot_state = Dict()

    def __init__(self):
        # initialize everything to be the equivalent of None. It would seem
        # that the first accessing of an Atom instance attribute causes the
        # creation of that attribute, thus triggering the @observe decorator.
        with self.suppress_notifications():
            self.column_models = {}
            self.scalar_columns = []
            self.line_columns = []
            self.image_columns = []
            self.volume_columns = []
            self.data_muxer = None
            self.header = None
            self.header_id = ''
            self.info = 'No run header received yet'
            self.new_data_callbacks = []

    @observe('header')
    def _run_header_changed(self, changed):
        logger.debug('Run header has been changed, creating a new data_muxer')
        self.info = 'Run {}'.format(self.header.scan_id)
        with self.suppress_notifications():
            self.data_muxer = None
        self.event_queue = EventQueue(self.header)
        self.get_new_data()
        # change the dataframe first
        self.dataframe = self.data_muxer.to_sparse_dataframe()
        # then change the id
        # (The scalar_model depends on this order being dataframe then id)
        self.header_id = self.header.run_start_id

    def new_run_header(self, changed):
        """Observer function for the `header` attribute of the SearchModels
        """
        self.header = changed['value']

    def get_new_data(self):
        """Hit the dataportal to first see if there is new data and, if so,
        grab it
        """
        logger.debug('getting new data from the data broker')
        self.event_queue.update()
        events = self.event_queue.get()
        if self.data_muxer is None:
            # this will automatically trigger the key updating
            data_muxer = DataMuxer()
            data_muxer.default_upsample = self.upsample
            data_muxer.default_downsample = self.downsample
            data_muxer.append_events(events)
            self.data_muxer = data_muxer
        else:
            self.data_muxer.append_events(events)
            self.dataframe = self.data_muxer.to_sparse_dataframe()
            # update the column information
            self._verify_column_info()
            try:
                for data_cb in self.new_data_callbacks:
                    data_cb()
            except IndexError:
                ttime.sleep(1)
                self.get_new_data()


    @observe('binning_column')
    def _binning_column_changed(self, changed):
        # make sure that the binning column combo box is kept in sync with the
        # actually selected value
        try:
            binning_index = self.binning_options.index(self.binning_column)
        except ValueError:
            binning_index = 0
        self.binning_index = binning_index

    @observe('norm_column')
    def _norm_column_changed(self, changed):
        # if oldvalue exists and it is not None, reset its value to the
        # non-normalized state
        old_norm_col = changed.get('oldvalue', None)
        if old_norm_col is None or old_norm_col == '':
            return
        logger.debug('old norm col: %s', old_norm_col)
        if old_norm_col == 'None':
            for name, model in self.column_models.items():
                model.can_be_normalized = True
        elif old_norm_col is not None and old_norm_col != 'None':
            self.column_models[old_norm_col].can_be_normalized = True
        new_norm_col = changed.get('value', None)
        if new_norm_col is None or new_norm_col == 'None' or new_norm_col == '':
            # disable all normalization check boxes
            for name, model in self.column_models.items():
                model.can_be_normalized = False
            return
        self.column_models[new_norm_col].is_being_normalized = False
        self.column_models[new_norm_col].can_be_normalized = False
        # make sure that the norm column combo box is kept in sync with the
        # actually selected value
        try:
            norm_index = self.norm_options.index(self.norm_column)
        except ValueError:
            norm_index = 0
        self.norm_index = norm_index

    @observe('data_muxer')
    def _new_muxer(self, changed):
        # data_muxer object has been changed. Remake the columns
        logger.debug('new data muxer received')
        self._verify_column_info()

    def perform_binning(self):
        logger.debug('binning column: %s', self.binning_column)
        if not (self.binning_column is None or self.binning_column == 'None'):
            # rebin the data
            dataframe = self.data_muxer.bin_on(self.binning_column)
            # normalize the new dataframe
            self._normalize_all(dataframe)
        else:
            dataframe = self.data_muxer.to_sparse_dataframe()
        # trigger the magic message passing cascade by assigning a new
        # dataframe to the instance data_frame attribute
        self.dataframe = dataframe

    def _normalize_all(self, dataframe):
        logger.debug('data frame before normalizing all')
        norm_cols = [col_name for col_name, col_model
                     in self.column_models.items()
                     if col_model.is_being_normalized]
        for col in dataframe.columns:
            if col[0] in norm_cols:
                dataframe[col] /= dataframe[(self.norm_column, 'val')]

        logger.debug('data frame after normalizing all')


    def normalize(self, column_name, should_be_normalized):
        """
        Parameters
        ----------
        column_name : string
            The column name to un/normalize
        should_be_normalized : bool
            If the column name should be normalized(True) or
            unnormalized (False)
        """
        if self.dataframe is None:
            logger.debug('Data must be binned before it can be normalized')
            return
        logger.debug('data frame before normalization')

        to_be_normalized = None
        for col in self.dataframe.columns:
            if col[0] == column_name:
                to_be_normalized = col
                break

        if should_be_normalized:
            self.dataframe[col] /= self.dataframe[(self.norm_column, 'val')]
        else:
            self.dataframe[col] *= self.dataframe[(self.norm_column, 'val')]
        logger.debug('data frame after normalization')
        # self.dataframe = combined

    def _verify_column_info(self):
        logger.debug('verifying column information')
        updated_cols = []
        for col_name, col_model in self.column_models.items():
            muxer_col_info = self.data_muxer.col_info.get(col_name, None)
            if muxer_col_info:
                # if the values are the same, no magic updates happen, otherwise
                # the UI gets magically updated
                col_model.dim = muxer_col_info.ndim
                col_model.name = muxer_col_info.name
                col_model.upsample = muxer_col_info.upsample
                col_model.downsample = muxer_col_info.downsample
                col_model.shape = muxer_col_info.shape
                col_model.data_muxer = self.data_muxer
                updated_cols.append(col_name)
            else:
                # remove the column model
                self.column_models.pop(col_name)
        for col_name, col_info in self.data_muxer.col_info.items():
            if col_name in updated_cols:
                # column has already been accounted for, move on to the next one
                continue
            # insert a new column model
            self.column_models[col_name] = ColumnModel(
                data_muxer=self.data_muxer, dim=col_info.ndim,
                name=col_name, shape=col_info.shape, upsample=col_info.upsample,
                downsample=col_info.downsample)
        self._update_column_sortings()

    def _update_column_sortings(self):
        logger.debug('updating column sortings')
        mapping = {0: set(), 1: set(), 2: set(), 3: set()}
        for col_name, col_model in self.column_models.items():
            mapping[col_model.dim].add(col_model)

        column_models = self.column_models
        # update the column key lists, if necessary
        self.scalar_columns = []
        self.line_columns = []
        self.image_columns = []
        self.volume_columns = []
        self.column_models = {}
        self.binning_options = []
        self.norm_options = []

        self.scalar_columns = list(mapping[0])
        self.line_columns = list(mapping[1])
        self.image_columns = list(mapping[2])
        self.volume_columns = list(mapping[3])
        self.column_models = column_models

        # set the GUI elements to be visible/hidden if there are/aren't any
        # column_models
        self.scalar_columns_visible = len(self.scalar_columns) != 0
        self.line_columns_visible = len(self.line_columns) != 0
        self.image_columns_visible = len(self.image_columns) != 0
        self.volume_columns_visible = len(self.volume_columns) != 0
        self.binning_options = ['None'] + list(column_models.keys())
        self.norm_options = ['None'] + list(column_models.keys())
