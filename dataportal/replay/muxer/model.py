"""Module that defines the all Models related to DataMuxer manipulations"""

import six
from collections import deque
from atom.api import (Atom, Typed, List, Range, Dict, observe, Str, Enum, Int,
                      Bool, ReadOnly, Tuple, Float)
from dataportal.muxer.api import DataMuxer
from dataportal.broker import simple_broker
from dataportal.broker.struct import BrokerStruct
from dataportal.muxer.data_muxer import ColSpec


def get_events(run_header):
    return simple_broker.get_events_by_run(run_header)


class ColumnModel(Atom):
    """Atom implementation of dataportal.muxer.data.ColSpec
    """
    name = Str()
    dim = Int()
    data_muxer = Typed(DataMuxer)
    upsample = Enum('linear', *ColSpec.upsampling_methods)
    downsample = Enum('mean', *ColSpec.downsampling_methods)
    _shape = Tuple()

    def __init__(self, data_muxer, dim, name, shape, upsample=None,
                 downsample=None):
        self.name = name
        self.dim = dim
        self.data_muxer = data_muxer
        self.shape = shape
        if upsample is None or upsample is 'None':
            upsample = 'linear'
        if downsample is None or downsample is 'None':
            downsample = 'mean'
        self.upsample = upsample
        self.downsample = downsample

    @observe('upsample', 'downsample')
    def sampling_changed(self, changed):
        print('Old data_muxer col_info: {}'.format(self.data_muxer.col_info[self.name]))
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
        print('New data_muxer col_info: {}'.format(self.data_muxer.col_info[self.name]))

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
    run_header: dataportal.broker.struct.BrokerStruct
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

    scalar_columns_visible: atom.scalars.Bool
        Instructs the GUI to show/hide the scalar info
    line_columns_visible: atom.scalars.Bool
        Instructs the GUI to show/hide the line info
    image_columns_visible: atom.scalars.Bool
        Instructs the GUI to show/hide the image info
    volume_columns_visible: atom.scalars.Bool
        Instructs the GUI to show/hide the volume info

    info : atom.scalars.Str
        A short string describing the `data_muxer` attribute of the Atom
        MuxerModel

    new_data_callbacks: atom.list.List
        List of callbacks that care when the data_muxer gets new data.
        Callback functions should expect no information to be passed.
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

    data_muxer = Typed(DataMuxer)
    run_header = Typed(BrokerStruct)
    info = Str()

    new_data_callbacks = List()

    update_rate = Int(1000) # in ms

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
            self.run_header = None
            self.info = 'No run header received yet'
            self.new_data_callbacks = []

    @observe('run_header')
    def run_header_changed(self, changed):
        print('Run header has been changed, creatomg a new data_muxer')
        self.info = 'Run {}'.format(self.run_header.id)
        with self.suppress_notifications():
            self.data_muxer = None
        self.get_new_data()

    def get_new_data(self):
        """Hit the dataportal to first see if there is new data and, if so,
        grab it
        """
        print('getting new data from the data broker')
        events = simple_broker.get_events_by_run(self.run_header, None)
        if self.data_muxer is None:
            # this will automatically trigger the key updating
            self.data_muxer = DataMuxer.from_events(events)
        else:
            self.data_muxer.append_events(events)
            for data_cb in self.new_data_callbacks:
                data_cb()
            # update the column information
            self._verify_column_info()
            for data_cb in self.new_data_callbacks:
                data_cb()

    @observe('data_muxer')
    def new_muxer(self, changed):
        # data_muxer object has been changed. Remake the columns
        print('new data muxer received')
        self._verify_column_info()

    def _verify_column_info(self):
        print('verifying column information')
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
            print(col_info)
            self.column_models[col_name] = ColumnModel(
                data_muxer=self.data_muxer, dim=col_info.ndim,
                name=col_name, shape=col_info.shape)
        self._update_column_sortings()

    def _update_column_sortings(self):
        print('updating column sortings')
        mapping = {0: set(), 1: set(), 2: set(), 3: set()}
        for col_name, col_model in self.column_models.items():
            mapping[col_model.dim].add(col_model)

        # update the column key lists, if necessary
        self.scalar_columns = []
        self.line_columns = []
        self.image_columns = []
        self.volume_columns = []

        self.scalar_columns = list(mapping[0])
        self.line_columns = list(mapping[1])
        self.image_columns = list(mapping[2])
        self.volume_columns = list(mapping[3])

        # set the GUI elements to be visible/hidden if there are/aren't any
        # column_models
        self.scalar_columns_visible = len(self.scalar_columns) != 0
        self.line_columns_visible = len(self.line_columns) != 0
        self.image_columns_visible = len(self.image_columns) != 0
        self.volume_columns_visible = len(self.volume_columns) != 0
