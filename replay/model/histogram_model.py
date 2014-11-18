__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range,
                      Float, Typed)
import numpy as np
from matplotlib.figure import Figure, Axes
from matplotlib.lines import Line2D
from matplotlib import colors
import enaml
from enaml.qt.qt_application import QtApplication
from bubblegum.backend.mpl.cross_section_2d import CrossSection
import logging
from skxray import core
logger = logging.getLogger(__name__)


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
    line_artist = Typed(Line2D)
    name = Str()

    def __init__(self, name=None):
        name = "histogram"
        self._fig = Figure(figsize=(1,1))
        self._ax = self._fig.add_subplot(111)
        self.line_artist, = self._ax.plot([], [], label=name,
                                            drawstyle="steps-mid")

    def set_img(self, img):
        """Update the data stored in line_artist

        Parameters
        ----------
        img : np.ndarray
            Image to compute the histogram for
        """
        vals, bins = np.histogram(img.ravel(), bins=100)
        bins = core.bin_edges_to_centers(bins)
        self.line_artist.set_data(bins, vals)
        self.replot()

    def set_data(self, x, y):
        """Update the data stored in line_artist

        Parameters
        ----------
        x : np.ndarray
        y : np.ndarray
        """
        self.line_artist.set_data(x, y)
        self.replot()

    def replot(self):
        self._ax.relim(visible_only=True)
        self._ax.autoscale_view(tight=True)
        self._fig.canvas.draw()



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