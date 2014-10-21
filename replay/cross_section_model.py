__author__ = 'edill'

from atom.api import Atom, List, observe, Bool, Enum, Str, Int, Range, Float
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
import enaml
from enaml.qt.qt_application import QtApplication
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
    figure = Figure()
    # normalization routine to use
    norm = Enum([colors.Normalize, colors.LogNorm])
    # limit function to use
    limit_func = Str()
    # figure that contains the main axes and the two cross section views
    init_img = np.zeros((1000, 1000))
    cs = CrossSection(fig=figure, init_image=init_img)


    # PARAMETERS -- CONTROL DOCK
    # minimum value for the slider
    minimum = Range(low=0)
    # maximum value for the slider
    maximum = Int()
    # slider value
    image_index = Int()
    # auto-update image
    auto_update = Bool()

    img_min = np.min(init_img)
    img_max = np.max(init_img)
    disp_min = Float()
    disp_max = Float()

    # OBSERVATION METHODS
    @observe('image_index')
    def update_image(self, update):
        try:
            self.cs.update_image(self.data[self.image_index])
        except AttributeError:
            # thrown at initiation because the figure has not yet been
            # added to the canvas
            pass
    @observe('data')
    def update_limits(self, update):
        self.maximum = len(self.data)-1
        if self.image_index >= self.maximum:
            self.image_index = 0
        if self.auto_update:
            self.image_index = self.maximum-1
    @observe('cmap')
    def update_cmap(self, update):
        self.cs.update_cmap(self.cmap)
    @observe('interpolation')
    def update_interpolation(self, update):
        self.cs.update_interpolation(self.interpolation)
    @observe('limit_func')
    def update_limit(self, update):
        self.cs.set_limit_func(self.limit_func)


if __name__ == "__main__":
    with enaml.imports():
        from  replay.gui.cross_section_view import (
            CrossSectionView, CrossSectionControlsDock, CrossSectionMain)
    pixels = 1000
    data = [np.random.rand(pixels,pixels) for _ in range(10)]
    model = CrossSectionModel(data=data)
    app = QtApplication()
    view = CrossSectionView(model=model)

    # controls = CrossSectionControls(model=model)
    main = CrossSectionMain(model=model)
    main.show()
    model.cs.init()
    app.start()
