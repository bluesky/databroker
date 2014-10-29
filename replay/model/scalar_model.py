__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant)
import numpy as np
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from lmfit import Model
from matplotlib.lines import Line2D
import pandas as pd
import six
from ..pipeline.pipeline import DataMuggler
import logging
logger = logging.getLogger(__name__)


class ScalarModel(Atom):
    """
    ScalarModel is the model in the Model-View-Controller pattern that backs
    a scalar versus some x-value, i.e., an (x,y) plot.  ScalarModel requires
    a line artist
    Parameters
    ----------
    line_artist : mpl.lines.Line2D
        The line_artist that the ScalarModel is in charge of bossing around
    name : atom.scalars.Str
        The name of the data set represented by this ScalarModel
    """

    # name of the data set being plotted
    name = Str()
    # visibility of the data set on the canvas
    is_plotting = Bool()
    # if the data set can be shown on the canvas
    can_plot = Bool()
    # the visual representation of the scalar model (the view!)
    line_artist = Typed(Line2D)

    def __init__(self, line_artist, name):
        self.line_artist = line_artist
        self.is_plotting = line_artist.get_visible()
        self.can_plot = True
        self.name = name

    def set_data(self, x, y):
        """Update the data stored in line_artist

        Parameters
        ----------
        x : np.ndarray
        y : np.ndarray
        """
        self.line_artist.set_data(x, y)

    @observe('is_plotting')
    def set_visible(self, changed):
        self.line_artist.set_visible(changed['value'])
        try:
            self.line_artist.axes.figure.canvas.draw()
        except AttributeError:
            pass

    @observe('can_plot')
    def set_plottable(self, changed):
        self.is_plotting = changed['value']

    def get_state(self):
        """Obtain the state of all instance variables in the ScalarModel

        Returns
        -------
        state : str
            The current state of the ScalarModel
        """
        state = ""
        state += '\nname: {}'.format(self.name)
        state += '\nis_plotting: {}'.format(self.is_plotting)
        state += '\ncan_plot: {}'.format(self.can_plot)
        state += '\nline_artist: {}'.format(self.line_artist)
        return state


class ScalarCollection(Atom):
    """

    ScalarCollection is a bundle of ScalarModels. The ScalarCollection has an
    instance of a DataMuggler which notifies it of new data which then updates
    its ScalarModels. When instantiated, the data_muggler instance is asked
    for the names of its columns.  All columns which represent scalar values
    are then shoved into ScalarModels and the ScalarCollection manages the
    ScalarModels.

    Parameters
    ----------
    data_muggler : replay.pipeline.pipeline.DataMuggler
        The data manager backing the ScalarModel. The DataMuggler's new_data
        signal is connected to the notify_new_data function of the ScalarModel
        so that the ScalarModel can decide what to do when the DataMuggler
        receives new data.
    """
    scalar_models = Dict(key=Str(), value=ScalarModel)
    # current x-axis of the scalar_models
    x = Str()
    # location where the data is stored
    data_muggler = Typed(DataMuggler)
    # mpl
    _fig = Typed(Figure)
    _ax = Typed(Axes)

    def __init__(self, data_muggler):
        with self.suppress_notifications():
            super(ScalarCollection, self).__init__()
            self._fig = Figure(figsize=(1,1))
            self._ax = self._fig.add_subplot(111)
            # self._ax.hold()
            # stash the data muggler
            self.data_muggler = data_muggler
            self.data_muggler.new_data.connect(self.notify_new_data)
            self.x = self.data_muggler.keys()[0]
            alignable = self.data_muggler.align_against(self.x)
            for name, is_plottable in six.iteritems(alignable):
                line_artist,  = self._ax.plot([], [], label=name)
                self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                       name=name)
                self.scalar_models[name].can_plot = is_plottable
        self.x = self.data_muggler.keys()[1]

    @observe('x')
    def update_x(self, changed):
        # check with the muggler for the columns that can be plotted against
        sliceable = self.data_muggler.align_against(self.x)
        for name, scalar_model in six.iteritems(self.scalar_models):
            if not sliceable[name]:
                # turn off the plotting and disable the check box
                scalar_model.is_plotting = False
                scalar_model.can_plot = False
            else:
                # enable the check box but don't turn on the plotting
                scalar_model.can_plot = True
        self._ax.set_xlabel(self.x)
        self.get_new_data_and_plot()

    def print_state(self):
        for model_name, model in six.iteritems(self.scalar_models):
            print(model.get_state())

    def notify_new_data(self, new_data):
        """ Function to call when there is new data in the data muggler

        Parameters
        ----------
        new_data : list
            List of names of updated columns from the data muggler
        """
        if self.x in new_data:
            # update all the data in the line plot
            self.get_new_data_and_plot()
        else:
            # find out which new_data keys overlap with the data that is
            # supposed to be shown on the plot
            intersection = [_ for _ in list(self.scalar_models)
                            if _ in new_data]
            self.get_new_data_and_plot(intersection)

    def get_new_data_and_plot(self, y_names=None):
        """
        Get the data from the data muggler for column `data_name` sampled
        at the time_stamps of `VariableModel.x`

        Parameters
        ----------
        data_name : list, optional
            List of the names of columns in the data muggler. If None, get all
            data from the data muggler
        """
        # self.print_state()
        if y_names is None:
            y_names = list(six.iterkeys(self.scalar_models))
        time, data = self.data_muggler.get_values(ref_col=self.x,
                                                  other_cols=y_names)
        ref_data = data.pop(self.x)
        if self.scalar_models[self.x].is_plotting:
            self.scalar_models[self.x].set_data(x=ref_data, y=ref_data)
        for dname, dvals in six.iteritems(data):
            self.scalar_models[dname].set_data(x=ref_data, y=dvals)
        self.plot()

    def plot(self):
        """
        Recompute the limits, rescale the view and redraw the canvas
        """
        try:
            arts, labs = zip(*[(v.line_artist, k)
                               for k, v in six.iteritems(self.scalar_models)
                               if v.line_artist.get_visible()])
            self._ax.legend(arts, labs)
            self._ax.relim(visible_only=True)
            self._ax.autoscale_view(tight=True)
            self._fig.canvas.draw()
        except AttributeError as ae:
            # should only happen once
            pass
