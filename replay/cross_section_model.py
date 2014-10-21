__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
import logging
logger = logging.getLogger(__name__)


class CrossSectionModel(Atom):
    """ Back-end for the Cross Section viewer and its control panel

    """
    # PARAMETERS -- VIEWER
    # List of 2-D images
    data = List()
    # interpolation routine to use
    interpolation = Str()
    # color map to use
    cmap = Str()
    # Matplotlib figure to draw the cross section on
    figure = Typed(Figure)
    # figure = Figure()
    # normalization routine to use
    norm = Enum([colors.Normalize, colors.LogNorm])
    # limit function to use
    limit_func = Str()
    cs = Typed(CrossSection)


    # PARAMETERS -- CONTROL DOCK
    # minimum value for the slider
    minimum = Range(low=0)
    # maximum value for the slider
    num_images = Int()
    # slider value
    image_index = Int()
    # auto-update image
    auto_update = Bool()

    # absolute minimum of the currently selected image
    img_min = Float()
    # absolute minimum of the currently selected image
    img_max = Float()
    # currently displayed minimum of the currently selected image
    disp_min = Float()
    # currently displayed maximum of the currently selected image
    disp_max = Float()

    def __init__(self, data):
        with self.suppress_notifications():
            self.figure = Figure()
            self.cs = CrossSection(fig=self.figure)
            if data is None:
                data = [np.random.random((1000, 1000)),]

            self.image_index = 0
            self.img_max = np.max(data[self.image_index])
            self.img_min = np.min(data[self.image_index])
        self.data = data

    # OBSERVATION METHODS
    @observe('image_index')
    def update_image(self, update):
        print('self.image_index: {}'.format(self.image_index))
        # try:
        self.cs.update_image(self.data[self.image_index])
        # except AttributeError:
            # thrown at initiation because the figure has not yet been
            # added to the canvas
    @observe('data')
    def update_num_images(self, update):
        print("len(self.data): {}".format(len(self.data)))
        # update the number of images
        self.num_images = len(self.data)-1
        if self.image_index >= self.num_images:
            # this would be the case when a completely new image stack is thrown
            # at this model
            self.image_index = 0
        if self.auto_update:
            # move to the last image in the stack
            self.image_index = self.num_images
        # repaint the canvas with a new set of images
        self.cs.update_image(self.data[self.image_index])
    @observe('cmap')
    def update_cmap(self, update):
        self.cs.update_cmap(self.cmap)
    @observe('interpolation')
    def update_interpolation(self, update):
        self.cs.update_interpolation(self.interpolation)
    @observe('limit_func')
    def update_limit(self, update):
        self.cs.set_limit_func(self.limit_func)
