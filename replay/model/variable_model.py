__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from lmfit import Model
import six
from ..pipeline.pipeline import DataMuggler
from .line_model import LineModel
import logging
logger = logging.getLogger(__name__)

class VariableModel(Atom):
    # value to use as the x-axis
    x = Str()
    # y value to fit against
    fit_name = Str()
    # list of y values to plot
    y = List()
    # variables that this model knows about
    vars = List()

    y_to_plot = Dict(value=Bool())
    # y-values of the fit
    fit_data = List()
    # the model has values to fit
    has_fit = Bool(False)
    # toggle to show or hide the fit
    plot_fit = Bool(False)
    # line model that backs the line plot
    line_model = Typed(LineModel)
    # location where the data is stored
    data_muggler = Typed(DataMuggler)

    def __init__(self, data_muggler, line_model=None):
        super(VariableModel, self).__init__()
        self.fit_data = []
        # stash the data muggler
        self.data_muggler = data_muggler
        self.vars = self.data_muggler.keys() + ['time',]
        self.x = self.vars[0]
        self.y_to_plot = dict.fromkeys(self.vars, False)
        self.line_model = line_model
        # connect the new data signal of the muggler to the new data processor
        # of the VariableModel
        self.data_muggler.new_data.connect(self.notify_new_data)

    @observe('x')
    def update_x(self, changed):
        self.get_new_data_and_plot(self.y)
        self.print_state()

    @observe('fit')
    def update_fit(self, changed):
        pass

    @observe('fit_data')
    def update_fit_data(self, changed):
        if self.fit_data is not None:
            self.has_fit = True

    def print_state(self):
        print('\n\n---printing state---\n')
        print('x: {}'.format(self.x))
        print('y: {}'.format(self.y))
        print('ploty: {}'.format(self.y_to_plot))
        print('vars: {}'.format(self.vars))
        print("fit: {}".format(self.fit_name))

    def update_y_list(self, is_checked, var_name):
        self.y = [var for var, is_enabled
                in six.iteritems(self.y_to_plot) if is_enabled]
        if is_checked:
            # get the data and add a new line to the plot
            self.get_new_data_and_plot([var_name, ])
        elif self.line_model is not None:
            # remove the line from the plot
            self.line_model.remove_xy(var_name)

    def notify_new_data(self, new_data):
        """ Function to call when there is new data in the data muggler

        Parameters
        ----------
        new_data : list
            List of names of updated columns from the data muggler
        """
        if self.x in new_data:
            # update all the data in the line plot
            self.get_new_data_and_plot(self.y)
        else:
            # find out which new_data keys overlap with the data that is
            # supposed to be shown on the plot
            intersection = [_ for _ in self.y if _ in new_data]
            self.get_new_data_and_plot(intersection)


    def get_new_data_and_plot(self, y_names):
        """
        Get the data from the data muggler for column `data_name` sampled
        at the time_stamps of `VariableModel.x`

        Parameters
        ----------
        data_name : list
            List of the names of columns in the data muggler
        """
        print("get_new_data_and_plot")
        self.print_state()
        ref_col = self.x
        if self.x == 'time':
            ref_col = self.vars[0]
        if y_names and self.x is not "":
            time, data = self.data_muggler.get_values(ref_col=ref_col,
                                                other_cols=y_names)
            if self.x != 'time':
                ref_data = data.pop(self.x)
            else:
                ref_data = time
            print('data from muggler: \ntime: {}\nx: {}\ny: {}'
                  ''.format(time, ref_data, data))
            for y_name, y_data in six.iteritems(data):
                # add xy data to the line model named `y_name`
                if self.line_model is not None:
                    self.line_model.add_xy(ref_data, y_data, y_name)
        if self.plot_fit and self.line_model is not None:
            # plot the fit
            self.line_model.add_xy(self.ref_data, self.fit_data, self.fit_name)