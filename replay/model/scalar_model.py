__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Constant, Coerced)
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
import six
from ..pipeline.pipeline import DataMuggler
from datetime import datetime
import logging
from .fitting_model import MultiFitController
logger = logging.getLogger(__name__)


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
    scalar_models = Dict(key=Str(), value=ScalarModel)
    data_muggler = Typed(DataMuggler)
    x = Str()
    col_names = List(item=str)

    # MPL PLOTTING STUFF
    _fig = Typed(Figure)
    _ax = Typed(Axes)

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

    def __init__(self, data_muggler, multi_fit_controller):
        with self.suppress_notifications():
            super(ScalarCollection, self).__init__()
            self.data_muggler = data_muggler
            self.multi_fit_controller = multi_fit_controller
            self._fig = Figure(figsize=(1,1))
            self._ax = self._fig.add_subplot(111)
            # self._ax.hold()
            # connect the signals from the muggler to the appropriate slots
            # in this class
            self.data_muggler.new_data.connect(self.notify_new_data)
            self.data_muggler.new_columns.connect(self.notify_new_column)
            # get the column names with dimensionality equal to zero
            self.col_names = self.data_muggler.keys(dim=0)
            self.col_names.append('fit')
            # default to the first column name
            self.x = self.col_names[0]
            # get the alignability of the columns that this model cares about
            alignable = self.data_muggler.align_against(self.x, self.col_names)
            for name, is_plottable in six.iteritems(alignable):
                # create a new line artist and scalar model
                line_artist, = self._ax.plot([], [], label=name)
                self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                       name=name,
                                                       can_plot=is_plottable,
                                                       is_plotting=True)
            # add the fit
            name = 'fit'
            line_artist, = self._ax.plot([], [], label=name)
            self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                   name=name,
                                                   can_plot=True,
                                                   is_plotting=False)
            self._last_update_time = datetime.utcnow()
        self.update_x(None)
        self.redraw_type = 's'

    @observe('x')
    def update_x(self, changed):
        # check with the muggler for the columns that can be plotted against
        sliceable = self.data_muggler.align_against(self.x)
        for name, scalar_model in six.iteritems(self.scalar_models):
            if name == 'fit':
                continue
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
        alignable = self.data_muggler.align_against(self.x, self.col_names)
        for name, is_plottable in six.iteritems(alignable):
            if name in new_columns and not self.data_muggler.col_dims[name]:
                line_artist,  = self._ax.plot([], [], label=name)
                self.scalar_models[name] = ScalarModel(line_artist=line_artist,
                                                       name=name)
                self.scalar_models[name].can_plot = is_plottable

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
        if self.x in new_data:
            # update all the data in the line plot
            intersection = list(self.scalar_models)
        else:
            # find out which new_data keys overlap with the data that is
            # supposed to be shown on the plot
            intersection = []
            for model_name, model in six.iteritems(self.scalar_models):
                if model.is_plotting and model.name in new_data:
                    intersection.append(model.name)
        if redraw:
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

        y_names = set(y_names)
        valid_names = set(k for k, v in six.iteritems(
                                 self.data_muggler.align_against(self.x))
                         if v)

        other_cols = list(y_names & valid_names)
        print('y_names: {}'.format(y_names))
        print('valid_names: {}'.format(valid_names))
        print('other_cols: {}'.format(other_cols))
        time, data = self.data_muggler.get_values(ref_col=self.x,
                                                  other_cols=other_cols)
        ref_data = data.pop(self.x)
        if self.scalar_models[self.x].is_plotting:
            self.scalar_models[self.x].set_data(x=ref_data, y=ref_data)
        for dname, dvals in six.iteritems(data):
            self.scalar_models[dname].set_data(x=ref_data, y=dvals)

        # manage the fitting
        if self.fit_target is not '':
            target_data = ref_data
            if self.fit_target != self.x:
                target_data = data[self.fit_target]
            self.multi_fit_controller.set_xy(x=ref_data.values, y=target_data.values)
        if self.multi_fit_controller.guess:
            self.multi_fit_controller.do_guess()
        if self.multi_fit_controller.autofit:
            self.multi_fit_controller.fit()
        try:
            self.scalar_models['fit'].set_data(x=ref_data.values,
                                               y=self.multi_fit_controller.best_fit)
        except RuntimeError:
            # thrown when x and y are not the same length
            pass
        self.plot()
        self.update_rate = "{0:.2f} s<sup>-1</sup>".format(float(
            self._num_updates) / (datetime.utcnow() -
                                 self._last_update_time).total_seconds())
        self._num_updates = 0
        self._last_update_time = datetime.utcnow()

    def plot(self):
        """
        Recompute the limits, rescale the view, reformat the legend and redraw
        the canvas
        """
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
            self._fig.canvas.draw()
        except AttributeError as ae:
            # should only happen once
            pass
