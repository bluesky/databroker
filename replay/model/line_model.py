__author__ = 'edill'

from atom.api import Atom, Bool, Typed, Dict
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import six
import logging
logger = logging.getLogger(__name__)

class LineModel(Atom):
    """ A class representing line data

    """
    xy = Dict()

    draw_single_line = Bool(False)

    # mpl setup
    _fig = Typed(Figure)
    _ax = Typed(Axes)

    def __init__(self, xy=None):
        self._fig = Figure()
        self._ax = self._fig.add_subplot(111)
        if xy is None:
            xy = {}
        self.xy = xy

    def add_xy(self, x, y, name):
        """

        Add a new xy pair to plot

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
        """

        Remove the xy pair specified by 'name' from the LineModel and

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
        # self._ax.cla()
        for name, xy in six.iteritems(self.xy):
            self._ax.plot(xy[0], xy[1], label=name)
        try:
            self._ax.figure.canvas.draw()
        except AttributeError:
            # should only occur once
            pass
