__author__ = 'edill'

from atom.api import Atom, observe, Typed, Dict, ForwardTyped
import numpy as np
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import six
import logging
from pims import FramesSequence
logger = logging.getLogger(__name__)

class ImageModel(Atom):
    """

    ImageModel is the model in Model-View-Controller that backs the ImageView
    which is basically just mpl.plot. The ScalarModel is bossed around by the
    ScalarController.  Instances of this class get instantiated with an
    instance of data_muggler which serves as the data back-end. When
    instantiated, the ScalarModel asks the data_muggler instance which of its
    data sets are scalars versus some index. Those data sets can be managed by
    the ScalarModel, shown by the ScalarView and bossed around by the
    ScalarController.

    Parameters
    ----------
    data_muggler : replay.pipeline.pipeline.DataMuggler
        The data manager backing the ScalarModel. The DataMuggler's new_data
        signal is connected to the notify_new_data function of the ScalarModel
        so that the ScalarModel can decide what to do when the DataMuggler
        receives new data.
    """
    # some sort of sliceable object
    data = Typed(FramesSequence)
    _fig = Typed(Figure)
    _ax = Typed(Axes)

    def __init__(self, data=None):
        super(ImageModel, self).__init__()
        # mpl setup
        self._fig = Figure()
        self._ax = self._fig.add_subplot(111)
        self._ax.hold(False)
        if data is not None:
            self.data = data
            self._ax.imshow(data)
        else:
            self._ax.imshow(np.random.rand((100,100)))

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
