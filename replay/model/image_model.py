__author__ = 'edill'

from atom.api import Atom, observe, Typed
import numpy as np
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import six
import logging
logger = logging.getLogger(__name__)

class ImageModel(Atom):
    data = Typed(np.ndarray)
    _fig = Typed(Figure)
    _ax = Typed(Axes)

    def __init__(self):
        super(ImageModel, self).__init__()
        # mpl setup
        self._fig = Figure()
        self._ax = self._fig.add_subplot(111)
        self._ax.imshow([[np.random.rand()]])
        self._ax.hold(False)

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

    def set_data(self, data):
        self.data = data
