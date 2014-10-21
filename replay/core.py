__author__ = 'edill'

from atom.api import Atom, observe, Bool, Typed
import numpy as np
from matplotlib.figure import Figure
import logging
logger = logging.getLogger(__name__)

class LineModel(Atom):
    """ A class representing line data

    """
    x = Typed(np.ndarray)

    y = Typed(np.ndarray)

    draw_single_line = Bool(False)

    _fig = Figure()

    _ax = _fig.add_subplot(111)

    _changed_x = Bool(False)
    _changed_y = Bool(False)

    def set_xy(self, x, y):
        """

        Set x and y values

        Parameters
        ----------
        x : list
            x-values
        y : list
            y-values
        """
        self.x = x
        self.y = y

    @observe('draw_single_line')
    def _is_holding(self, change):
        print('self.single_line: {0}'.format(self.draw_single_line))
        self._ax.hold(self.draw_single_line)

    @observe('x')
    def _new_x(self, change):
        print('x changed')
        _changed_x = Bool(True)
        self._attempt_to_plot()

    @observe('y')
    def _new_y(self, change):
        print('y changed')
        _changed_y = Bool(True)
        self._attempt_to_plot()

    def _attempt_to_plot(self):
        # make sure the axes are the same length
        if self.x is None or self.y is None:
            return
        if self.x.shape == self.y.shape:
            self._ax.plot(self.x, self.y)
            self._changed_x = False
            self._changed_y = False
            try:
                self._ax.figure.canvas.draw()
            except AttributeError:
                # should only occur once
                pass
        elif self._changed_x is True and self._changed_y is True:
            logger.debug('x and y have both been changed but are '
                         'not the same length')

class ImageModel(Atom):
    data = Typed(np.ndarray)
    # mpl setup
    _fig = Figure()
    _ax = _fig.add_subplot(111)
    _im = _ax.imshow([[np.random.rand()]])
    _ax.hold(False)

    @observe('data')
    def _new_data(self, change):
        print('data changed')
        data_arr = np.asarray(self.data)
        self._ax.imshow(data_arr)
        try:
            self._ax.figure.canvas.draw()
        except AttributeError:
            # should only occur once
            pass
