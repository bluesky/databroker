__author__ = 'edill'

from collections import OrderedDict
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant, Coerced)
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
import six
from ...muggler.data import DataMuggler
from datetime import datetime
import logging
from copy import copy
from pprint import pprint
from .fitting_model import MultiFitController
logger = logging.getLogger(__name__)
import numpy as np
import random


nodata_str = "data_muggler is None"


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
    can_plot = Bool()
    line_artist = Typed(Line2D)

    def __init__(self, line_artist, **kwargs):
        self.line_artist = line_artist
        self.is_plotting = line_artist.get_visible()
        print(kwargs)
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

    Attributes
    ----------
    data_muggler : replay.pipeline.pipeline.DataMuggler
        The data manager backing the ScalarModel. The DataMuggler's new_data
        signal is connected to the notify_new_data function of the ScalarModel
        so that the ScalarModel can decide what to do when the DataMuggler
        receives new data.
    scalar_models : atom.Dict
        The collection of scalar_models that the ScalarCollection knows about
    col_names : atom.List
        The names of the data sets that are in the DataMuggler
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
    data_muggler = Typed(DataMuggler)
    # The scan id of this data set
    scan_id = Int()
    # name of the x axis
    x = Str()
    # index of x in col_names
    # x_index = col_names.index(x)
    x_index = Int()

    # name of the column to align against
    alignment_col = Str()
    x_is_data = Bool(True)
    x_is_time = Bool(False)
    # name of all columns that the data muggler knows about
    col_names = List()

    # ESTIMATING
    # the current set of data to perform peak estimates for
    estimate_target = Str()
    # the result of the estimates, stored as a dictionary
    # The list of peak parameters to plot
    estimate_stats = Typed(OrderedDict)
    estimate_plot = List()
    # the index of the data set to perform estimates for
    # estimate_index = col_names.index(estimate_target)
    estimate_index = Int()

    # NORMALIZING
    normalize_target = Str()
    # should the data be normalized?
    normalize = Bool(False)

    # MPL PLOTTING STUFF
    _fig = Typed(Figure)
    _ax = Typed(Axes)
    # configuration properties for the 1-D plot
    _conf = Typed(ScalarConfig)

    # FITTING
    fit_target = Str()
    multi_fit_controller = Typed(MultiFitController)

    # CONTROL OF THE PLOT UPDATE SPEED
    redraw_every = Float(default=1)
    redraw_type = Enum('max rate', 's')
    update_rate = Str()
    # the last time that the plot was updated
    _last_update_time = Typed(datetime)
    # the last frame that the plot was updated
    _last_update_frame = Int()
    # the number of times that `notify_new_data` has been called since the last
    # update
    _num_updates = Int()

    def __init__(self):
        with self.suppress_notifications():
            super(ScalarCollection, self).__init__()
            # plotting initialization
            self._fig = Figure(figsize=(1,1))
            self._fig.set_tight_layout(True)
            self._ax = self._fig.add_subplot(111)
            self._conf = ScalarConfig(self._ax)
            self.redraw_type = 's'
            self.estimate_plot = ['cen', 'x_at_max']
            self.estimate_stats = OrderedDict()

    def init_scalar_models(self):
        self.scalar_models.clear()
        line_artist, = self._ax.plot([], [], label=nodata_str)
        self.scalar_models[nodata_str] = ScalarModel(line_artist=line_artist,
                                               name=nodata_str,
                                               can_plot=True,
                                               is_plotting=True)

    @observe('data_muggler')
    def update_datamuggler(self, changed):
        with self.suppress_notifications():
            if self.data_muggler is None:
                self.col_names = [nodata_str]
                self.x = nodata_str
                self.estimate_target = self.x
                self.estimate_index = self.col_names.index(self.x)
                self.normalize_target = self.x
                self.alignment_col = self.x
                self.init_scalar_models()
                return
            # connect the signals from the muggler to the appropriate slots
            # in this class
            self.data_muggler.new_data.connect(self.notify_new_data)
            self.data_muggler.new_columns.connect(self.notify_new_column)
            # get the column names with dimensionality equal to zero
            self.col_names = self.data_muggler.keys(dim=0)
            self.col_names.append('fit')
            # default to the first column name
            self.x = self.col_names[0]
            self.estimate_target = self.x
            self.estimate_index = self.col_names.index(self.estimate_target)
            self.normalize_target = self.x
            self.alignment_col = self.col_names[0]
            # blow away scalar models
            self.scalar_models.clear()
            self._ax.cla()
            # print('self.col_names: {}'.format(self.col_names))
            # print('self.x: {}'.format(self.x))
            # print('self.alignment_col: {}'.format(self.alignment_col))
            # get the alignability of the columns that this model cares about
            alignable = self.data_muggler.align_against(self.alignment_col,
                                                        self.col_names)
            # print('\nalignable\n---------')
            # pprint(alignable)
            # print('\\alignable')

            for name, is_plottable in six.iteritems(alignable):
                # create a new line artist and scalar model
                line_artist, = self._ax.plot([], [], label=name, marker='D')
                self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                       name=name,
                                                       can_plot=is_plottable,
                                                       is_plotting=False)
            # add the fit
            name = 'fit'
            line_artist, = self._ax.plot([], [], label=name)
            self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                   name=name,
                                                   can_plot=True,
                                                   is_plotting=False)
            # add the estimate
            name = 'peak stats'
            line_artist, = self._ax.plot([], [], 'ro', label=name,
                                           markersize=15)
            self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                   name=name,
                                                   can_plot=True,
                                                   is_plotting=True)
        # self._conf.title = 'Scan id: {}. {}'.format(self.scan_id,
        #                                             datetime.utcnow())

        self._last_update_time = datetime.utcnow()
        self.col_names = []
        self.col_names = self.data_muggler.keys(dim=0) + ['fit', 'peak stats']
        self.alignment_col = self.col_names[0]
        self.x = self.col_names[0]
        self.estimate_target = self.x
        self.estimate_index = self.col_names.index(self.estimate_target)
        self.normalize_target = self.x

    @observe('x_is_data', 'x_is_time')
    def update_x_axis(self, changed):
        with self.suppress_notifications():
            if self.x_is_time:
                self.x_is_data = False
            elif self.x_is_data:
                self.x_is_time = False
        # print('x is data: {}\tx is time: {}'.format(self.x_is_data, self.x_is_time))
        if self.x_is_data or self.x_is_time:
            self.get_new_data_and_plot()

    @observe('x')
    def update_x(self, changed):
        self._conf.xlabel = self.x
        self.get_new_data_and_plot()
        self.x_index = self.col_names.index(self.x)

    @observe('alignment_col')
    def update_alignment_col(self, changed):
        # check with the muggler for the columns that can be plotted against
        sliceable = self.data_muggler.align_against(self.alignment_col)
        for name, scalar_model in six.iteritems(self.scalar_models):
            if name == 'fit' or name == 'peak stats':
                continue
            if not sliceable[name]:
                # turn off the plotting and disable the check box
                scalar_model.is_plotting = False
                scalar_model.can_plot = False
            else:
                # enable the check box but don't turn on the plotting
                scalar_model.can_plot = True
        self.get_new_data_and_plot()

    @observe('estimate_plot')
    def update_estimate(self, changed):
        self.reformat_view()

    @observe('estimate_target')
    def update_estimate_target(self, changed):
        self.estimate_index = self.col_names.index(self.estimate_target)

    @observe('normalize')
    def update_normalize(self, changed):
        self.get_new_data_and_plot()

    def print_state(self):
        """Print the, uh, state
        """
        for model_name, model in six.iteritems(self.scalar_models):
            print(model.get_state())

    def notify_new_column(self, new_columns):
        """Function to call when there is a new column in the data muggler

        Parameters
        ----------
        new_columns: list
            The new column name that the data muggler knows about
        """
        scalar_cols = self.data_muggler.keys(dim=0)
        alignable = self.data_muggler.align_against(self.alignment_col,
                                                    self.col_names)
        for name, is_plottable in six.iteritems(alignable):
            if name in new_columns and not self.data_muggler.col_dims[name]:
                line_artist,  = self._ax.plot([], [], label=name)
                self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                       name=name)
                self.scalar_models[name].can_plot = is_plottable

    def format_number(self, number):
        return '{:.5f}'.format(number)

    def estimate(self):

        """Return a dictionary of the vital stats of a 'peak'"""
        stats = OrderedDict()
        print('self.fit_target: {}'.format(self.fit_target))
        print('self.alignment_col: {}'.format(self.alignment_col))
        print('self.x: {}'.format(self.x))
        other_cols = [self.x, self.estimate_target]
        if self.normalize:
            other_cols.append(self.normalize_target)
        print('other_cols: {}'.format(other_cols))
        time, data = self.data_muggler.get_values(ref_col=self.alignment_col,
                                                  other_cols=other_cols)
        x = np.asarray(data[self.x])

        y = np.asarray(data[self.estimate_target])

        if self.normalize:
            y = y / np.asarray(data[self.normalize_target])

        # print('x, len(x): {}, {}'.format(x, len(x)))
        # print('y, len(y): {}, {}'.format(y, len(y)))

        fn = lambda num: '{:.5}'.format(num)
        # Center of peak
        stats['ymin'] = y.min()
        stats['ymax'] = y.max()
        stats['avg_y'] = fn(np.average(y))
        stats['x_at_ymin'] = x[y.argmin()]
        stats['x_at_ymax'] = x[y.argmax()]

        # Calculate CEN from derivative
        zero_cross = np.where(np.diff(np.sign(y - (stats['ymax'] + stats['ymin'])/2)))[0]
        if zero_cross.size == 2:
            stats['cen'] = (fn(x[zero_cross].sum() / 2),
                            fn((stats['ymax'] + stats['ymin'])/2))
        elif zero_cross.size == 1:
            stats['cen'] = x[zero_cross[0]]
        if zero_cross.size == 2:
            fwhm = x[zero_cross]
            stats['width'] = fwhm[1] - fwhm[0]
            stats['fwhm_left'] = (fn(fwhm[0]), fn(y[zero_cross[0]]))
            stats['fwhm_right'] = (fn(fwhm[1]), fn(y[zero_cross[1]]))

        # Center of mass
        stats['center_of_mass'] = fn((x * y).sum() / y.sum())
        #
        #
        # extra_models = []
        # for model_name, model in six.iteritems(self.scalar_models):
        #     if model_name in self.estimate_stats:
        #         line_artist = self.scalar_models[model_name].line_artist
        #         self._ax.lines.remove(line_artist)
        #         line_artist.remove()
        #         del line_artist
        #         print(line_artist)
        #         model = self.scalar_models.pop(model_name)
        #         model = None
        #
        # # create new line artists
        # for name, val in six.iteritems(stats):
        #     line_artist, = self._ax.axvline(label=name, color = 'r',
        #                                     linewidth=2,
        #                                     x=val)
        #     self.scalar_models[name] = ScalarModel(line_artist=line_artist,
        #                                            name=name,
        #                                            can_plot=True,
        #                                            is_plotting=False)
        # self.col_names = []
        # self.col_names = list(six.iterkeys(self.scalar_models))

        # trigger the automatic update of the GUI
        self.estimate_stats = OrderedDict()
        self.estimate_stats = stats

    def notify_new_data(self, new_data):
        """ Function to call when there is new data in the data muggler

        Parameters
        ----------
        new_data : list
            List of names of updated columns from the data muggler
        """

        self._num_updates += 1
        redraw = False
        if self.redraw_type == 's':
            if ((datetime.utcnow() - self._last_update_time).total_seconds()
                    >= self.redraw_every):
                redraw = True
            else:
                # do nothing
                pass
        elif self.redraw_type == 'max rate':
            redraw = True
        if self.alignment_col in new_data:
            # update all the data in the line plot
            y_names = list(self.scalar_models)
        else:
            # find out which new_data keys overlap with the data that is
            # supposed to be shown on the plot
            y_names = []
            for model_name, model in six.iteritems(self.scalar_models):
                if model.is_plotting and model.name in new_data:
                    y_names.append(model.name)
        if redraw:
            self.get_new_data_and_plot(y_names)
            self.estimate()

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
        if self.data_muggler is None:
            return
        # self.print_state()
        if y_names is None:
            y_names = list(six.iterkeys(self.scalar_models))

        y_names = set(y_names)
        valid_names = set(k for k, v in six.iteritems(
                                 self.data_muggler.align_against(
                                     self.alignment_col))
                          if v)

        other_cols = list(y_names & valid_names)
        other_cols.append(self.normalize_target)
        other_cols = list(set(other_cols))
        # print('y_names: {}'.format(y_names))
        # print('valid_names: {}'.format(valid_names))
        # print('other_cols: {}'.format(other_cols))
        time, data = self.data_muggler.get_values(ref_col=self.alignment_col,
                                                  other_cols=other_cols)
        ref_data = data.pop(self.x)
        if self.normalize:
            norm_data = data.pop(self.normalize_target)
        # switch between x axis as data and x axis as time
        if self.x_is_data:
            ref_data_vals = ref_data.values
        else:
            ref_data_vals = time
        # print('ref_data_vals: {}'.format(ref_data_vals))

        if self.scalar_models[self.x].is_plotting:
            self.scalar_models[self.x].set_data(x=ref_data_vals, y=ref_data)
        for dname, dvals in six.iteritems(data):
            # print('{}: {}'.format(dname, id(self.scalar_models[dname])))
            if self.normalize:
                dvals = dvals/norm_data
            self.scalar_models[dname].set_data(x=ref_data_vals, y=dvals)

        # manage the fitting
        if self.fit_target is not '':
            target_data = ref_data
            if self.fit_target != self.x:
                target_data = data[self.fit_target]
            self.multi_fit_controller.set_xy(x=ref_data_vals,
                                             y=target_data.values)
        if self.multi_fit_controller.guess:
            self.multi_fit_controller.do_guess()
        if self.multi_fit_controller.autofit:
            self.multi_fit_controller.fit()
        try:
            self.scalar_models['fit'].set_data(
                x=ref_data_vals, y=self.multi_fit_controller.best_fit)
        except RuntimeError:
            # thrown when x and y are not the same length
            pass
        self.reformat_view()
        self.update_rate = "{0:.2f} s<sup>-1</sup>".format(float(
            self._num_updates) / (datetime.utcnow() -
                                 self._last_update_time).total_seconds())
        self._num_updates = 0
        self._last_update_time = datetime.utcnow()
        # except KeyError:
        #     pass
        # self._ax.axvline(x=self.estimate_stats['cen'], linewidth=2, color='r')
        # self._ax.axvline(x=self.estimate_stats['x_at_max'], linewidth=4,
        #                  color='k')

    def reformat_view(self):
        """
        Recompute the limits, rescale the view, reformat the legend and redraw
        the canvas
        """

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
            # data muggler hasn't been created yet
            pass
        try:
            legend_pairs = [(v.line_artist, k)
                            for k, v in six.iteritems(self.scalar_models)
                            if v.line_artist.get_visible()]
            if legend_pairs:
                arts, labs = zip(*legend_pairs)
                self._ax.legend(arts, labs)
            else:
                self._ax.legend(legend_pairs)
            self._ax.relim(visible_only=True)
            self._ax.autoscale_view(tight=True)
            self._ax.grid(self._conf.grid)
            self._ax.set_ylabel(self._conf.ylabel)
            self._ax.set_xlabel(self._conf.xlabel)
            self._ax.set_title(self._conf.title)
            self._fig.canvas.draw()
        except AttributeError as ae:
            # should only happen once
            pass
