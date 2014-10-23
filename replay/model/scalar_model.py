__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant)
import numpy as np
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from lmfit import Model
import pandas as pd
import six
from ..pipeline.pipeline import DataMuggler
import logging
logger = logging.getLogger(__name__)

class ScalarModel(Atom):
    xy = Dict()

    draw_single_line = Bool(False)

    # flag that defines the x-axis as time or not time
    time = Bool(False)

    # mpl setup
    _fig = Typed(Figure)
    _ax = Typed(Axes)

    # SCALAR METHODS
    # value to use as the x-axis
    x = Str()
    # y value to fit against
    fit_name = Str()
    # list of y values to plot
    y = List()
    # scalar variables that this model knows about
    scalar_vars = List()
    # line variables that this model knows about
    line_vars = List()
    # image variables that this model knows about
    image_vars = List()
    # volume variables that this model knows about
    volume_vars = List()

    x_time = Constant('time')
    # y values to plot. dictionary keyed on values from y
    y_to_plot = Dict(value=Bool())
    # y-values of the fit
    fit_data = List()
    # the model has values to fit
    has_fit = Bool(False)
    # toggle to show or hide the fit
    plot_fit = Bool(False)
    # location where the data is stored
    data_muggler = Typed(DataMuggler)

    def __init__(self, data_muggler):
        with self.suppress_notifications():
            super(ScalarModel, self).__init__()
            self._fig = Figure(figsize=(1,1))
            self._ax = self._fig.add_subplot(111)
            self.fit_data = []
            # stash the data muggler
            self.data_muggler = data_muggler
            self.scalar_vars = self.data_muggler.keys() + [self.x_time, ]
            self.y_to_plot = dict.fromkeys(self.scalar_vars, False)
            # connect the new data signal of the muggler to the new data processor
            # of the VariableModel
            self.data_muggler.new_data.connect(self.notify_new_data)
            # do some init magic
            self.x = self.scalar_vars[1]
            self.y_to_plot['max'] = True
            self.update_y_list(is_checked=True, var_name='max')
        self.get_new_data_and_plot(self.y)

    @observe('x')
    def update_x(self, changed):
        print(changed)
        if self.x == self.x_time:
            # let line_model know that its x-axis is time so that it can use
            # pandas great built-in time plotter
            self.time = True
            self.xy = {}
        else:
            # let line_model use matplotlib's plotter
            self.time = False
            self.xy = {}
        # grab new data from the data muggler
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
        print('vars: {}'.format(self.scalar_vars))
        print("fit: {}".format(self.fit_name))

    def update_y_list(self, is_checked, var_name):
        """

        Should only get called from the view when a y-value checkbox gets
        checked. This function re-computes

        Parameters
        ----------
        is_checked : bool
            Is the checkbox that called this checked?
        var_name : str
            Name of the variable whose checkbox got checked
        """
        self.y = [var for var, is_enabled
                in six.iteritems(self.y_to_plot) if is_enabled]
        if is_checked:
            # get the data and add a new line to the plot
            self.get_new_data_and_plot([var_name, ])
        elif self.scalar_model is not None:
            # remove the line from the plot
            self.remove_xy(var_name)


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
        if y_names and self.x is not "":
            if self.x == self.x_time:
                self.time = True
                for col_name in y_names:
                    x, y = self.data_muggler.get_column(col_name)
                    self.add_xy(x, y, col_name)

            else:
                time, data = self.data_muggler.get_values(ref_col=self.x,
                                                          other_cols=y_names)
                ref_data = data.pop(self.x)
                for y_name, y_data in six.iteritems(data):
                    # add xy data to the line model named `y_name`
                    self.add_xy(ref_data, y_data, y_name)
        if self.plot_fit and self.scalar_model is not None:
            # plot the fit
            self.add_xy(self.ref_data, self.fit_data, self.fit_name)

    def set_xy(self, x, y):
        """Set the xy data to plot. Will only draw a single line

        Parameters
        ----------
        x : list
            x-values
        y : list
            y-values
        """
        self.xy = {'data': (x, y)}
        self.plot()

    def add_xy(self, x, y, name):
        """Add a new xy pair to plot

        Parameters
        ----------
        x : list
            x-values
        y : list
            y-values
        name : str
            Name of the data set
        """
        # check the length of x and y
        if len(x) != len(y):
            raise ValueError('x and y must be the same length. len(x) = {}, '
                             'len(y) = {}'.format(len(x), len(y)))
        # empty the dictionary if draw_single_line is on
        if self.draw_single_line:
            self.xy = {}

        self.xy[name] = (x, y)
        self.plot()

    def remove_xy(self, name):
        """Remove the xy pair specified by 'name' from the LineModel

        Parameters
        ----------
        name : str
            Name of the data set

        Return
        ------
        xy : tuple
            Tuple of (x, y) where x and y are lists or ndarrays
            Returns none if this LineModel does not understand `name`
        """
        xy = self.xy.pop(name, None)
        self.plot()
        return xy

    def plot(self):
        self._ax.cla()
        if self.time:
            series_dict = {col_name: pd.Series(data=xy[1], index=xy[0])
                           for col_name, xy in six.iteritems(self.xy)}
            df = pd.DataFrame(series_dict)
            df.plot(ax=self._ax)
        else:
            for name, xy in six.iteritems(self.xy):
                self._ax.plot(xy[0], xy[1], label=name)
        try:
            self._ax.figure.canvas.draw()
        except AttributeError:
            # should only occur once
            pass