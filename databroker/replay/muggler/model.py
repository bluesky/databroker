"""Module that defines the all Models related to DataMuggler manipulations"""

import six
from collections import deque
from atom.api import (Atom, Typed, List, Range, Dict, observe, Str, Enum, Int,
                      Bool)
from databroker.muggler.api import DataMuggler
from databroker.broker import simple_broker
from databroker.broker.struct import BrokerStruct
from databroker.muggler.data import ColSpec


def get_events(run_header):
    return simple_broker.get_events_by_run(run_header)


class ColumnModel(Atom):
    """

    """
    name = Str()
    __dim = Int()
    muggler = Typed(DataMuggler)
    upsample = Enum('None', *ColSpec.upsampling_methods)
    downsample = Enum('None', *ColSpec.downsampling_methods)

    def __init__(self, muggler, dim, upsample, downsample, name):
        with self.suppress_notifications():
            self.name = name
            self.__dim = dim
            self.muggler = muggler
            self.upsample = upsample
            self.downsample = downsample

    @observe('upsample', 'downsample')
    def sampling_changed(self, changed):
        print('Old muggler col_info: {}'.format(self.muggler._col_info[self.name]))
        upsample = self.upsample
        if upsample == 'None':
            upsample = None
        downsample = self.downsample
        if downsample == 'None':
            downsample = None
        # replace the column info on the muggler with the new upsample
        # or downsample
        self.muggler._col_info[self.name] = ColSpec(self.name, self.__dim,
                                                    upsample, downsample)
        print('New muggler col_info: {}'.format(self.muggler._col_info[self.name]))

    def __repr__(self):
        return ('ColumnModel(name={}, muggler={}, dim={}, upsample={}, '
                'downsample={})'.format(self.name, self.muggler, self.__dim,
                                        self.upsample, self.downsample))

# Existing questions
# Muggler knows the dimensionality of each data column
# What are the options for upsampling/downsampling?
class MugglerModel(Atom):
    """Class that defines the Model for the data muggler

    Attributes
    ----------
    muggler : databroker.muggler.api.DataMuggler
    """
    scalar_columns = List(item=ColumnModel)
    line_columns = List(item=ColumnModel)
    image_columns = List(item=ColumnModel)
    volume_columns = List(item=ColumnModel)

    scalar_columns_visible = Bool(False)
    line_columns_visible = Bool(False)
    image_columns_visible = Bool(False)
    volume_columns_visible = Bool(False)

    muggler = Typed(DataMuggler)
    run_header = Typed(BrokerStruct)
    info = Str()

    @observe('run_header')
    def run_header_changed(self, changed):
        self.info = 'Run {}'.format(self.run_header.id)
        self.get_new_data()

    def get_new_data(self):
        """Hit the databroker to first see if there is new data and, if so,
        grab it
        """
        events = simple_broker.get_events_by_run(self.run_header, None)
        if self.muggler is None:
            self.muggler = DataMuggler.from_events(events)
        else:
            self.muggler.append_events(events)
        self.compare_keys()

    def compare_keys(self):
        mapping = {0: [], 1: [], 2: [], 3: []}
        for col_name, col_spec in six.iteritems(self.muggler.col_info):
            # grab the dimensionality, upsampling rules and downsampling rules
            # from the col_spec named tuple
            col_dim = col_spec.ndim
            col_up = str(col_spec.upsample)
            col_down = str(col_spec.downsample)
            if not col_name in mapping[col_dim]:
                column_model = ColumnModel(muggler=self.muggler,
                                           name=col_name, upsample=col_up,
                                           downsample=col_down,
                                           dim=col_dim)
                mapping[col_dim].append(column_model)
        # update the instance varaibles, if necessary
        if set(self.scalar_columns) != set(mapping[0]):
            self.scalar_columns = mapping[0]
        if set(self.line_columns) != set(mapping[0]):
            self.line_columns = mapping[1]
        if set(self.image_columns) != set(mapping[1]):
            self.image_columns = mapping[2]
        if set(self.volume_columns) != set(mapping[2]):
            self.volume_columns = mapping[3]

        # set the GUI elements to be visible/hidden if there are/aren't any
        # column_models
        self.scalar_columns_visible = len(self.scalar_columns) != 0
        self.line_columns_visible = len(self.line_columns) != 0
        self.image_columns_visible = len(self.image_columns) != 0
        self.volume_columns_visible = len(self.volume_columns) != 0


    def update_keys_new(self):
        mapping = {0: {'attr': self.scalar_columns, 'updated': False},
                   1: {'attr': self.line_columns, 'updated': False},
                   2: {'attr': self.image_columns, 'updated': False},
                   3: {'attr': self.volume_columns, 'updated': False}}
        for col_name, col_ndim in six.iteritems(self.muggler.col_ndim):
            if not col_name in mapping[col_ndim]['attr']:
                mapping[col_ndim]['attr'].append(col_name)
                mapping[col_ndim]['updated'] = True
        for col_name, col_ndim in six.iteritems(self.muggler.col_ndim):
            if mapping[col_ndim]['updated']:
                new_list = mapping[col_ndim]['attr']
                mapping[col_ndim]['attr'] = []
                mapping[col_ndim]['attr'] = new_list
        for dim, lst in six.iteritems(mapping):
            print('{}: {}'.format(dim, lst))

