from collections import OrderedDict
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant, Coerced)
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
import six
from ...muxer.data_muxer import DataMuxer
from datetime import datetime
import logging
import numpy as np

from metadatastore.api import Document
from pandas import DataFrame

__author__ = 'edill'
logger = logging.getLogger(__name__)

nodata_str = "data_muxer is None"


class ScalarConfig(Atom):
    """
    ScalarConfig holds various configuration parameters for the 1-D plot

    Attributes
    ----------
    title : str
        The title of the plot
    xlabel  : str
        The label on the x axis
    ylabel : str
        The label on the y axis
    grid : bool
        Show the grid on the 1-D plot
    """
    title = Str()
    xlabel = Str()
    ylabel = Str()
    grid = Bool(True)
    _ax = Typed(Axes)

    def __init__(self, ax, *args, **kwargs):
        super(ScalarConfig, self).__init__(*args, **kwargs)
        self._ax = ax
        self._ax.set_title(self.title)
        self._ax.set_xlabel(self.xlabel)
        self._ax.set_ylabel(self.ylabel)
        self._ax.grid(self.grid)

    @observe('title')
    def title_changed(self, changed):
        self._ax.set_title(self.title)
        print('{}: {}'.format(changed['name'], changed['value']))
        self.replot()

    @observe('xlabel')
    def xlabel_changed(self, changed):
        self._ax.set_xlabel(self.xlabel)
        print('{}: {}'.format(changed['name'], changed['value']))
        self.replot()

    @observe('ylabel')
    def ylabel_changed(self, changed):
        self._ax.set_ylabel(self.ylabel)
        print('{}: {}'.format(changed['name'], changed['value']))
        self.replot()

    @observe('grid')
    def grid_changed(self, changed):
        self._ax.grid(self.grid)
        print('{}: {}'.format(changed['name'], changed['value']))
        self.replot()

    def replot(self):
        self._ax.relim(visible_only=True)
        self._ax.autoscale_view(tight=True)
        if self._ax.figure.canvas is not None:
            self._ax.figure.canvas.draw()


class ScalarModel(Atom):
    """
    ScalarModel is the model in the Model-View-Controller pattern that backs
    a scalar versus some x-value, i.e., an (x,y) plot.  ScalarModel requires
    a line artist

    Attributes
    ----------
    line_artist : matplotlib.lines.Line2D
        The visual representation of the scalar model (the view!)
    name : atom.scalars.Str
        The name of the data set represented by this ScalarModel
    is_plotting : atom.Bool
        Visibility of the data set on the canvas
    can_plot : atom.Bool
        If the data set can be shown on the canvas

    """
    name = Str()
    is_plotting = Bool()
    line_artist = Typed(Line2D)

    def __init__(self, line_artist, **kwargs):
        self.line_artist = line_artist
        self.is_plotting = line_artist.get_visible()
        # print(kwargs)
        for name, val in six.iteritems(kwargs):
            setattr(self, name, val)

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
        # print('{} is visible: {}'.format(self.name, self.is_plotting))
        self.line_artist.set_visible(changed['value'])
        try:
            self.line_artist.axes.figure.canvas.draw()
        except AttributeError:
            pass

    @property
    def state(self):
        """Obtain the state of all instance variables in the ScalarModel

        Returns
        -------
        state : str
            The current state of the ScalarModel
        """
        state = ""
        state += '\nname: {}'.format(self.name)
        state += '\nis_plotting: {}'.format(self.is_plotting)
        state += '\nline_artist: {}'.format(self.line_artist)
        return state


class ScalarCollection(Atom):
    """

    ScalarCollection is a bundle of ScalarModels. The ScalarCollection has an
    instance of a DataMuxer which notifies it of new data which then updates
    its ScalarModels. When instantiated, the data_muxer instance is asked
    for the names of its columns.  All columns which represent scalar values
    are then shoved into ScalarModels and the ScalarCollection manages the
    ScalarModels.

    Attributes
    ----------
    data_muxer : replay.pipeline.pipeline.DataMuxer
        The data manager backing the ScalarModel. The DataMuxer's new_data
        signal is connected to the notify_new_data function of the ScalarModel
        so that the ScalarModel can decide what to do when the DataMuxer
        receives new data.
    scalar_models : atom.Dict
        The collection of scalar_models that the ScalarCollection knows about
    data_cols : atom.List
        The names of the data sets that are in the DataMuxer
    redraw_every : atom.Float
        The frequency with which to redraw the plot. The meaning of this
        parameter changes based on `redraw_type`
    redraw_type : {'max rate', 's'}
        Gives meaning to the float stored in `redraw_every`. Should be read as
        'Update the plot at a rate of `redraw_every` per `redraw_type`'. Since
        there are only the two options in `ScalarCollection`, it should be
        understood that the previous statement is only relevant when 's' is
        selected as the `redraw_type`. If `max_rate` is selected, then the plot
        will attempt to update itself as fast as data is coming in. Beware that
        this may cause significant performance issues if your data rate is
        > 20 Hz
    update_rate : atom.Str
        Formatted rate that new data is coming in.
    x : atom.Str
        The name of the x-axis that the `scalar_models` should be plotted
        against
    """
    # dictionary of lines that can be toggled on and off
    scalar_models = Dict(key=Str(), value=ScalarModel)
    # the thing that holds all the data
    dataframe = Typed(DataFrame)
    # name of the x axis
    x = Str()

    # name of the column to align against
    x_is_time = Bool(False)
    # name of all columns that the data muxer knows about
    data_cols = List()

    # MPL PLOTTING STUFF
    _fig = Typed(Figure)
    _ax = Typed(Axes)
    # configuration properties for the 1-D plot
    _conf = Typed(ScalarConfig)

    def __init__(self):
        with self.suppress_notifications():
            super(ScalarCollection, self).__init__()
            # plotting initialization
            self._fig = Figure(figsize=(1, 1))
            self._fig.set_tight_layout(True)
            self._ax = self._fig.add_subplot(111)
            self._conf = ScalarConfig(self._ax)

    def set_state(self, state_dict):
        try:
            x = state_dict.pop('x')
        except KeyError:
            # x is not present in the state_dict
            x = None
        if x is not None:
            self.x = x
        try:
            y = state_dict.get('y')
        except KeyError:
            # y is not present in the state_dict
            y = None
        if y is not None:
            for name, model in self.scalar_models.items():
                model.is_plotting = name in y

        for k, v in state_dict.items():
            setattr(self, k, v)

    def init_scalar_models(self):
        self._ax.cla()
        self.scalar_models.clear()
        line_artist, = self._ax.plot([], [], label=nodata_str)
        self.scalar_models[nodata_str] = ScalarModel(
            line_artist=line_artist, name=nodata_str, is_plotting=True)

    def new_dataframe(self, changed):
        self.dataframe = changed['value']

    @observe('dataframe')
    def dataframe_changed(self, changed):
        self.init_scalar_models()

        valid_cols= [col for col in self.dataframe.columns
                     if self.dataframe[col].dropna().values[0].shape == tuple()]

        # aggregate columns
        # create new scalar models
        for col_name in valid_cols:
            # create a new line artist and scalar model
            x = np.asarray(self.dataframe[col_name].index)
            y = np.asarray(self.dataframe[col_name].values)
            col_name = str(col_name)
            line_artist, = self._ax.plot(x, y, label=col_name, marker='D')
            self.scalar_models[col_name] = ScalarModel(
                line_artist=line_artist, name=col_name, is_plotting=True)

        self.data_cols = [str(col) for col in valid_cols]

    @observe('data_cols')
    def update_col_names(self, changed):
        print('data_cols changed: {}'.format(self.data_cols))

    @observe('x')
    def update_x(self, changed):
        print('x updated: {}'.format(self.x))
        self._conf.xlabel = self.x
        if not self.x:
            return
        self.get_new_data_and_plot()

    def set_state(self):
        plotx = getattr(self.header, 'plotx', None)
        ploty = getattr(self.header, 'ploty', None)

        if plotx:
            self.x = plotx

        print('plotx: {}'.format(plotx))
        print('ploty: {}'.format(ploty))

        for name, model in self.scalar_models.items():
            is_plt = bool(ploty is None or name in ploty)
            print(name, model, 'name in ploty: ', is_plt)
            self.scalar_models[name].is_plotting = is_plt

        if ploty:
            self.x_is_time = False

    def format_number(self, number):
        return '{:.5f}'.format(number)

    def get_new_data_and_plot(self, y_names=None):
        """
        Get the data from the data muxer for column `data_name` sampled
        at the time_stamps of `VariableModel.x`

        Parameters
        ----------
        data_name : list, optional
            List of the names of columns in the data muxer. If None, get all
            data from the data muxer
        """
        if self.dataframe is None:
            return
        if self.x_is_time:
            self.plot_by_time()
        else:
            self.plot_by_x()

    def plot_by_time(self):
        df = self.dataframe
        data_dict = {data_name: {'x': df[data_name].index.tolist(),
                                 'y': df[data_name].tolist()}
                     for data_name in df.columns
                     if data_name in self.data_cols}
        self._plot(data_dict)

    def plot_by_x(self):
        if not self.x:
            return

        df = self.dataframe
        x_axis = df[self.x].val.values
        data_dict = {data_name[0]: {'x': x_axis, 'y': df[data_name].tolist()}
                     for data_name in df}
        self._plot(data_dict)

    def _plot(self, data_dict):
        for dname, dvals in data_dict.items():
            if dname in self.data_cols:
                self.scalar_models[dname].set_data(dvals['x'], dvals['y'])
                # self.scalar_models[dname].is_plotting = True
        self.reformat_view()

    def reformat_view(self, *args, **kwargs):
        """
        Recompute the limits, rescale the view, reformat the legend and redraw
        the canvas
        """
        # ignore the args and kwargs. They are here so that any function can be
        # connected to this one

        x_data = []
        y_data = []
        try:
            y_val = self.estimate_stats['avg_y']
        except KeyError:
            y_val = 1
        for plot in self.estimate_plot:
            try:
                stats = self.estimate_stats[plot]
            except KeyError:
                continue
            try:
                stats_len = len(stats)
            except TypeError:
                stats_len = 1
            if stats_len == 2:
                x_data.append(stats[0])
                y_data.append(stats[1])
            else:
                x_data.append(stats)
                y_data.append(y_val)
        try:
            self.scalar_models['peak stats'].set_data(x=x_data, y=y_data)
        except KeyError:
            # data muxer hasn't been created yet
            pass
        try:
            legend_pairs = [(v.line_artist, k)
                            for k, v in six.iteritems(self.scalar_models)
                            if v.line_artist.get_visible()]
            if legend_pairs:
                arts, labs = zip(*legend_pairs)
                self._ax.legend(arts, labs).draggable()
            else:
                self._ax.legend(legend_pairs)
            self._ax.relim(visible_only=True)
            self._ax.autoscale_view(tight=True)
            self._ax.grid(self._conf.grid)
            self._ax.set_ylabel(self._conf.ylabel)
            self._ax.set_xlabel(self._conf.xlabel)
            self._ax.set_title(self._conf.title)
            self._fig.canvas.draw()
        # self._ax.figure.canvas.draw()
        # print('current figure id: {}'.format(id(self._fig)))
        except AttributeError:
            # should only happen once
            pass
