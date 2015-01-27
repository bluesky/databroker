from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range,
                      Float, Typed)
import numpy as np
from matplotlib.figure import Figure, Axes
from matplotlib.lines import Line2D
from matplotlib import colors as mcolors
import enaml
from enaml.qt.qt_application import QtApplication
from bubblegum.backend.mpl.cross_section_2d import (CrossSection,
                                                    fullrange_limit_factory,
                                                    absolute_limit_factory,
                                                    percentile_limit_factory)
import logging
from skxray import core
logger = logging.getLogger(__name__)

from matplotlib import cm

class HistogramModel(Atom):
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
    # MPL PLOTTING STUFF
    _fig = Typed(Figure)
    _ax = Typed(Axes)
    name = Str()
    limit_func = Typed(type(np.linspace))
    img = Typed(np.ndarray)
    cmap = Str()
    vals = Typed(np.ndarray)
    bins = Typed(np.ndarray)
    norm = Typed(mcolors.Normalize)
    min = Float()
    max = Float()
    mycmap = Typed(mcolors.Colormap)

    def __init__(self, limit_func=None):
        with self.suppress_notifications():
            if limit_func is None:
                limit_func = fullrange_limit_factory()
            self._fig = Figure(figsize=(1,1))
            self._ax = self._fig.add_subplot(111)
            self.limit_func = limit_func
        self.mycmap = cm.get_cmap('jet')
        self.img = np.random.random((100,100))


    @observe('cmap')
    def update_cmap(self, changed):
        self.mycmap = cm.get_cmap(self.cmap)
        self.replot_histogram(changed)

    # todo: Make this faster by utilizing `matplotlib.patches.Rectangle` as long
    # todo: as the number of bins doesn't change. If it does, blow away the old
    # todo: set of Rectangle patches and create a new set

    @observe('img', 'limit_func')
    def replot_histogram(self, changed):
        """Update the data stored in line_artist

        Parameters
        ----------
        img : np.ndarray
            Image to compute the histogram for
        """
        print('formatting histogram')
        if self.img is not None:
            self.min, self.max = self.limit_func(self.img)
            self.norm = mcolors.Normalize(vmin=self.min, vmax=self.max)
            bins = np.linspace(start=self.min, stop=self.max, num=100)
            print('num bins: {}\tmin bin: {}\tmax bin: {}\tmin: {}\tmax: {}'
                  ''.format(len(bins), np.min(bins), np.max(bins), self.min,
                  self.max))
            self.vals, self.bins = np.histogram(self.img.ravel(), bins=bins)
            self.bins = self.bins[:-1]
            self._ax.cla()
            colors = self.mycmap(self.norm(self.bins))
            self._ax.bar(left=self.bins, height=self.vals, color=colors, edgecolor='k',
                         width=np.average(np.diff(self.bins)))
                         # align='center')
            self.replot()
        print('formatting histogram complete')

    def replot(self):
        self._ax.relim(visible_only=True)
        self._ax.autoscale_view(tight=True)
        if self._fig.canvas is not None:
            self._fig.canvas.draw()

    def get_state(self):
        """Obtain the state of all instance variables in the ScalarModel

        Returns
        -------
        state : str
            The current state of the ScalarModel
        """
        state = ""
        state += '\nis_plotting: {}'.format(self.is_plotting)
        state += '\ncan_plot: {}'.format(self.can_plot)
        state += '\nline_artist: {}'.format(self.line_artist)
        return state