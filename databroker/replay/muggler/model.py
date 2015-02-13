"""Module that defines the all Models related to DataMuggler manipulations"""

import six
from collections import deque
from atom.api import (Atom, Typed, List, Range, Dict, observe, Str, Enum, Int,
                      Bool, ReadOnly, Tuple)
from databroker.muggler.api import DataMuggler
from databroker.broker import simple_broker
from databroker.broker.struct import BrokerStruct
from databroker.muggler.data import ColSpec


def get_events(run_header):
    return simple_broker.get_events_by_run(run_header)


class ColumnModel(Atom):
    """Atom implementation of databroker.muggler.data.ColSpec
    """
    name = Str()
    dim = Int()
    data_muggler = Typed(DataMuggler)
    upsample = Enum('None', *ColSpec.upsampling_methods)
    downsample = Enum('None', *ColSpec.downsampling_methods)
    shape = Tuple()

    def __init__(self, data_muggler, dim, upsample, downsample, name, shape):
        with self.suppress_notifications():
            self.name = name
            self.dim = dim
            self.data_muggler = data_muggler
            self.upsample = upsample
            self.downsample = downsample
            if shape is None:
                shape = tuple()
            self.shape = shape

    @observe('upsample', 'downsample')
    def sampling_changed(self, changed):
        print('Old data_muggler col_info: {}'.format(self.data_muggler.col_info[self.name]))
        # upsample = self.upsample
        # if upsample == 'None':
        #     upsample = None
        # downsample = self.downsample
        # if downsample == 'None':
        #     downsample = None
        # replace the column info on the data_muggler with the new upsample
        # or downsample
        self.data_muggler.col_info[self.name] = ColSpec(
            self.name, self.dim, self.shape, self.upsample, self.downsample)
        print('New data_muggler col_info: {}'.format(self.data_muggler.col_info[self.name]))

    def __repr__(self):
        return ('ColumnModel(name={}, data_muggler={}, dim={}, upsample={}, '
                'downsample={})'.format(self.name, self.data_muggler, self.dim,
                                        self.upsample, self.downsample))

class MugglerModel(Atom):
    """Class that defines the Model for the data muggler

    Attributes
    ----------
    data_muggler : databroker.muggler.api.DataMuggler
        The data_muggler holds the non-time-aligned data.  Upon asking the data_muggler
        to reformat its data into time-aligned bins, a dataframe is returned
    run_header: databroker.broker.struct.BrokerStruct
        The bucket of information from the data broker that contains all
        non-data information

    column_models : atom.dict.Dict
        Dictionary that is analogous to the col_info property of the
        databroker.muggler.data.DataMuggler object
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
        A short string describing the `data_muggler` attribute of the Atom
        MugglerModel

    new_column_callbacks: atom.list.List
        List of callbacks that care when the data_muggler gets new columns. Callback
        functions should expect a dictionary keyed on column dimensionality
        with values as lists of column names
    new_data_callbacks: atom.list.List
        List of callbacks that care when the data_muggler gets new data
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

    data_muggler = Typed(DataMuggler)
    run_header = Typed(BrokerStruct)
    info = Str()

    new_column_callbacks = List()
    new_data_callbacks = List()

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
            self.data_muggler = None
            self.run_header = None
            self.info = 'No run header received yet'
            self.new_column_callbacks = []
            self.new_data_callbacks = []

    @observe('run_header')
    def run_header_changed(self, changed):
        print('Run header has been changed, creatomg a new data_muggler')
        self.info = 'Run {}'.format(self.run_header.id)
        self.get_new_data()

    def get_new_data(self):
        """Hit the databroker to first see if there is new data and, if so,
        grab it
        """
        print('getting new data from the data broker')
        events = simple_broker.get_events_by_run(self.run_header, None)
        if self.data_muggler is None:
            # this will automatically trigger the key updating
            self.data_muggler = DataMuggler.from_events(events)
        else:
            self.data_muggler.append_events(events)
            for data_cb in self.new_data_callbacks:
                data_cb()
            # update the column information
            self._verify_column_info()

    @observe('data_muggler')
    def new_muggler(self, changed):
        # data_muggler object has been changed. Remake the columns
        print('new data muggler received')
        self._verify_column_info()

    def _verify_column_info(self):
        print('verifying column information')
        updated_cols = []
        for col_name, col_model in self.column_models.items():
            muggler_col_info = self.data_muggler.get(col_name, None)
            if muggler_col_info:
                # if the values are the same, no magic updates happen, otherwise
                # the UI gets magically updated

                col_model.dim = muggler_col_info.ndim
                col_model.name = muggler_col_info.name
                col_model.upsample = muggler_col_info.upsample
                col_model.downsample = muggler_col_info.downsample
                col_model.shape = muggler_col_info.shape
                col_model.data_muggler = self.data_muggler
                updated_cols.append(col_name)
            else:
                # remove the column model
                self.column_models.pop(col_name)
        for col_name, col_info in self.data_muggler.col_info.items():
            if col_name in updated_cols:
                # column has already been accounted for, move on to the next one
                continue
            # insert a new column model
            self.column_models[col_name] = ColumnModel(
                data_muggler=self.data_muggler, dim=col_info.ndim,
                upsample=col_info.upsample, downsample=col_info.downsample,
                name=col_name, shape=col_info.shape)
        self._update_column_sortings()

    def _update_column_sortings(self):
        print('updating column sortings')
        mapping = {0: set(), 1: set(), 2: set(), 3: set()}
        for col_name, col_model in self.column_models.items():
            mapping[col_model.dim].add(col_model)

        # update the column key lists, if necessary
        if set(self.scalar_columns) != set(mapping[0]):
            self.scalar_columns = list(mapping[0])
        if set(self.line_columns) != set(mapping[1]):
            self.line_columns = list(mapping[1])
        if set(self.image_columns) != set(mapping[2]):
            self.image_columns = list(mapping[2])
        if set(self.volume_columns) != set(mapping[3]):
            self.volume_columns = list(mapping[3])

        # set the GUI elements to be visible/hidden if there are/aren't any
        # column_models
        self.scalar_columns_visible = len(self.scalar_columns) != 0
        self.line_columns_visible = len(self.line_columns) != 0
        self.image_columns_visible = len(self.image_columns) != 0
        self.volume_columns_visible = len(self.volume_columns) != 0
