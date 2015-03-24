from collections import OrderedDict
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant, Coerced, Tuple)
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
import six
from ...muxer.data_muxer import DataMuxer
from datetime import datetime
import logging
import numpy as np
from metadatastore.api import Document
import pandas as pd
from ..persist import History
from dataportal import replay


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
        logger.debug('%s: %s', changed['name'], changed['value'])
        self.replot()

    @observe('xlabel')
    def xlabel_changed(self, changed):
        self._ax.set_xlabel(self.xlabel)
        logger.debug('%s: %s', changed['name'], changed['value'])
        self.replot()

    @observe('ylabel')
    def ylabel_changed(self, changed):
        self._ax.set_ylabel(self.ylabel)
        logger.debug('%s: %s', changed['name'], changed['value'])
        self.replot()

    @observe('grid')
    def grid_changed(self, changed):
        self._ax.grid(self.grid)
        logger.debug('%s: %s', changed['name'], changed['value'])
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
    is_plotting = Bool(False)
    line_artist = Typed(Line2D)

    def set_data(self, x, y):
        """Update the data stored in line_artist

        Parameters
        ----------
        x : np.ndarray
        y : np.ndarray
        """
        self.line_artist.set_data(x, y)

    @observe('line_artist')
    def _line_artist_changed(self, changed):
        if self.line_artist is None:
            return
        self.line_artist.set_visible(self.is_plotting)

    @observe('is_plotting')
    def _is_plotting_changed(self, changed):
        if self.line_artist is None:
            return
        self.line_artist.set_visible(self.is_plotting)
        try:
            self.line_artist.axes.figure.canvas.draw()
        except AttributeError:
            # only raised when the figure has not been added to a canvas.
            # Should really only happen once
            logger.debug('ScalarModel._is_plotting_changed: Figure has not '
                         'yet been added to canvas')

    @property
    def x(self):
        return self.line_artist.get_xdata()

    @property
    def y(self):
        return self.line_artist.get_ydata()

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


class ColumnModel(Atom):
    column_address = Typed(object) # really this is a tuple or a string
    dataframe = Typed(pd.DataFrame)

    def _launder(self, obj):
        return np.asarray(obj)

    @property
    def data(self):
        return self._launder(self.dataframe[self.column_address].values)

    @property
    def index(self):
        return self._launder(self.dataframe[self.column_address].index)

    @property
    def time(self):
        return self._launder(self._dataframe['time'])

    @property
    def name(self):
        if isinstance(self.column_address, six.string_types):
            return self.column_address
        return '-'.join(self.column_address)


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
    # dictionary of data for the pandas dataframe that backs the
    # ScalarCollection
    column_models = Dict(key=Str(), value=ColumnModel)
    # the thing that holds all the data
    dataframe = Typed(pd.DataFrame)
    # name of the x axis
    x = Str()
    # names of the currently plotting things on the y-axis
    y = List()
    # attribute needed to keep the x axis combo box selector in sync
    x_index = Int()

    # name of the column to align against
    x_is_index = Bool(True)
    # name of all columns that the data muxer knows about
    data_cols = List()

    # MPL PLOTTING STUFF
    _fig = Typed(Figure)
    _ax = Typed(Axes)
    # configuration properties for the 1-D plot
    _conf = Typed(ScalarConfig)
    # some id that replay uses as a key for plotting state
    dataframe_id = Str()
    # the sql database that keeps track of headers from run-to-run
    history = Typed(History)
    # tell replay to use the state from the last selected header
    use_ram_state = Bool(False)
    # tell replay to use the state from the last time this header was viewed
    use_disk_state = Bool(True)

    def __init__(self, history, **kwargs):
        self.history = history
        for k, v in kwargs.items():
            setattr(self, k, v)

        with self.suppress_notifications():
            super(ScalarCollection, self).__init__()
            # plotting initialization
            self._fig = Figure(figsize=(1, 1))
            self._fig.set_tight_layout(True)
            self._ax = self._fig.add_subplot(111)
            self._conf = ScalarConfig(self._ax)
            self.dataframe_id = ''

    @observe('dataframe_id')
    def dataframe_id_changed(self, changed):
        dataframe_id = changed['value']
        if dataframe_id is None or dataframe_id == 'None' or dataframe_id == '':
            dataframe_id = ''
        with self.suppress_notifications():
            self.dataframe_id = dataframe_id
        logger.debug('dataframe id in scalar model: %s', self.dataframe_id)
        try:
            logger.debug('getting state for dataframe id: %s', self.dataframe_id)
            state = self.history.get(six.text_type(self.dataframe_id))
            logger.debug('state retrieved for dataframe id: %s\nstate: %s', self.dataframe_id, state)
        except IndexError:
            # there are no entries in the db for 'state'
            logger.debug('no state found for dataframe id: %s', self.dataframe_id)
            state = {}
        if self.use_ram_state:
            # the state has already been correctly configured
            return
        elif self.use_disk_state and state:
            # update the plot with the data sets that were plotting last time
            self.__setstate__(state)
            y = state.get('y', None)
            if y:
                for name, model in self.scalar_models.items():
                    model.is_plotting = name in y
                self.get_new_data_and_plot()
            x = state.get('x', None)
            if x:
                self.x_index = self.scalar_models.keys().index(self.x)


    @observe('x', 'x_is_index', 'y')
    def save_plotting_state(self, changed):
        plotting_state = {'x': self.x, 'y': self.y, 'x_is_index': self.x_is_index}
        logger.debug('writing plotting state for id: [%s] ... %s',
            self.dataframe_id, plotting_state)
        replay.core.save_state(self.history, self.dataframe_id, plotting_state)

    def clear_scalar_models(self):
        self._ax.cla()
        self.data_cols = []
        self.column_models = {}
        self.scalar_models = {}

    def new_dataframe(self, changed):
        self.dataframe = changed['value']

    @observe('dataframe')
    def dataframe_changed(self, changed):
        if self.dataframe is not None:
            self.new_data()

    def new_data(self):
        old_plotting_values = [col_name for col_name, col_model in
                               self.scalar_models.items()
                               if col_model.is_plotting]
        old_x = self.x
        old_x_is_index = self.x_is_index

        scalar_cols = [col for col in self.dataframe.columns
                     if self.dataframe[col].dropna().values[0].shape == tuple()]
        # figure out if the dataframe has one or more levels of labels
        # for now these need to be handled differently
        if isinstance(self.dataframe.columns[0], six.string_types):
            # then the dataframe does not have hierarchical indexing
            self._do_magic(scalar_cols)
        elif isinstance(self.dataframe.columns[0], tuple):
            # then the dataframe has hierarchical indexing
            # self._do_nested_magic(scalar_cols)
            # but for now treat them the same...
            self._do_magic(scalar_cols)
        for scalar_name, scalar_model in self.scalar_models.items():
            scalar_model.is_plotting = scalar_name in old_plotting_values
        if old_x in self.scalar_models.keys():
            self.x = old_x
            self.x_is_index = old_x_is_index
        else:
            self.x = self.scalar_models.keys()[0]
            self.x_is_index = True
        self.get_new_data_and_plot()

    def _do_magic(self, scalar_cols):
        # create new scalar models
        scalar_models = {}
        column_models = {}
        for col_name, col_model in self.column_models.items():
            if col_model.column_address in scalar_cols:
                column_models[col_name] = self.column_models.pop(col_name)
                scalar_models[col_name] = self.scalar_models.pop(col_name)
        self.clear_scalar_models()
        for col_name in scalar_cols:
        #    if col_name in self.column_models or col_name in self.scalar_models:
        #        col_model = self.column_models[col_name]
        #        col_model.dataframe = self.dataframe
        #        self.scalar_models[col_name].set_data(col_model.index, col_model.data)
        #        continue
            # create a new line artist and scalar model
            column_model = ColumnModel(dataframe=self.dataframe,
                                       column_address=col_name)
            column_models[column_model.name] = column_model
            line_artist, = self._ax.plot([], [], label=column_model.name, marker='D')
            scalar_model = ScalarModel(line_artist=line_artist,
                                       is_plotting=False,
                                       name=column_model.name)
            scalar_models[scalar_model.name] = scalar_model
        # throw an empty list at data cols before using list comprehension to
        # set the new values. This is one method to trigger the Atom magic,
        # though I'm sure there is a better way to do it

        # determine if the column_models and scalar_models need to be updated
        #new_column_models = [col_name for col_name in column_models.keys()
        #                     if col_name not in self.column_models]
        #new_scalar_models = [scalar_name for scalar_name in scalar_models.keys()
        #                     if scalar_name not in self.scalar_models]
        #if len(new_column_models) > 0:
        self.column_models = column_models
        #if len(new_scalar_models) > 0:
        self.scalar_models = scalar_models
        self.data_cols = []
        self.data_cols = list({name.split('-')[0] for name in scalar_models.keys()})

    def _do_nested_magic(self, scalar_cols):
        pass

    @observe('data_cols')
    def update_col_names(self, changed):
        pass

    @observe('x')
    def update_x(self, changed):
        try:
            self.x_index = self.scalar_models.keys().index(self.x)
        except ValueError:
            self.x_index = 0
        self._conf.xlabel = self.x
        if not self.x:
            return
        self.get_new_data_and_plot()

    def get_new_data_and_plot(self):
        """Helper function to shunt plotting with the index or a column as x.
        """
        if self.dataframe is None:
            return
        if self.x_is_index:
            self.plot_by_index()
        else:
            self.plot_by_x()

    def plot_by_index(self):
        self._conf.xlabel = 'index (sequential counting numbers)'
        data_dict = {model_name: (model.index, model.data)
                     for model_name, model in self.column_models.items()}
        self._plot(data_dict)

    def plot_by_x(self):
        if not self.x:
            return
        self._conf.xlabel = self.x
        x_data = self.column_models[six.text_type(self.x)].data
        data_dict = {model_name: (x_data, model.data)
                     for model_name, model in self.column_models.items()}
        self._plot(data_dict)

    def _plot(self, data_dict):
        for model_name, xy_tuple in data_dict.items():
            self.scalar_models[model_name].set_data(*xy_tuple)
                # self.scalar_models[dname].is_plotting = True
        self.reformat_view()

    def reformat_view(self, *args, **kwargs):
        """
        Recompute the limits, rescale the view, reformat the legend and redraw
        the canvas
        """
        # ignore the args and kwargs. They are here so that any function can be
        # connected to this one

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
        except AttributeError:
            # should only happen once
            pass
