from __future__ import (print_function, absolute_import, division)
import six
from pims import FramesSequence
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed)
import numpy as np
import sys
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from ..pipeline.pipeline import DataMuggler, DmImgSequence
import logging
logger = logging.getLogger(__name__)

__author__ = 'edill'

class CrossSectionModel(Atom):
    """ Back-end for the Cross Section viewer and its control panel

    """
    # PARAMETERS -- VIEWER
    # List of 2-D images
    sliceable_data = Typed(DmImgSequence)
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
    # back end for plotting. cs holds a figure that paints the cross section
    # viewer
    cs = Typed(CrossSection)
    # name of the data set that this CrossSectionModel represents
    name = Str()
    # data muggler
    dm = Typed(DataMuggler)

    # PARAMETERS -- CONTROL DOCK
    # minimum value for the slider
    minimum = Range(low=0)
    # maximum value for the slider
    num_images = Int()
    # slider value
    image_index = Int(0)
    # auto-update image
    auto_update = Bool(False)

    # absolute minimum of the currently selected image
    img_min = Float()
    # absolute minimum of the currently selected image
    img_max = Float()
    disp_min = Float()
    # currently displayed maximum of the currently selected image
    disp_max = Float()

    def __init__(self, data_muggler, sliceable_data=None, name=None):
        with self.suppress_notifications():
            self.name = name
            self.figure = Figure()
            self.cs = CrossSection(fig=self.figure)
            self.cmap = self.cs._cmap
            self.interpolation = self.cs._interpolation
            self.dm = data_muggler
            if sliceable_data is None:
                sliceable_data = DmImgSequence(data_muggler=self.dm,
                                               data_name=self.name)
            self.image_index = 0
            self.auto_update = True
            # hook up the data muggler's 'I have new data' signal
            self.dm.new_data.connect(self.notify_new_data)
        self.sliceable_data = sliceable_data

    # slot to hook the data muggler into
    def notify_new_data(self, new_data):
        if self.name in new_data:
            self.num_images = len(self.sliceable_data)
            if self.auto_update:
                self.image_index = self.num_images-1

    def get_state(self):
        state = "Current state of CrossSectionModel"
        interesting_vars = ['sliceable_data', 'interpolation', 'cmap',
                            'figure', 'norm', 'limit_func', 'cs', 'name', 'dm',
                            'minimum', 'num_images', 'image_index',
                            'auto_update', 'img_min', 'img_max', 'disp_min',
                            'disp_max']
        for var in interesting_vars:
            try:
                state += "\n{}: {}".format(var, self.__getattribute__(var))
            except Exception as e:
                state += "\n{}: attribute not available because an exception " \
                         "was raised: {}".format(var, e)
        return state

    # OBSERVATION METHODS
    @observe('sliceable_data')
    def _update_sliceable_data(self, update):
        print('sliceable data updated')
        try:
            self.num_images = len(self.sliceable_data)
            self.img_max = np.max(self.sliceable_data[self.image_index])
            self.img_min = np.min(self.sliceable_data[self.image_index])
        except IndexError as ie:
            # thrown when the data frame doesn't understand the column name
            msg = ("Index Error thrown in _update_sliceable_data. Printing "
                   "current state of the cross section model\n")
            msg = self.get_state()
            print(msg)
    @observe('name')
    def _update_name(self, update):
        self.sliceable_data.data_name = self.name
    @observe('image_index')
    def _update_image(self, update):
        print('self.image_index: {}'.format(self.image_index))
        self.cs.update_image(self.sliceable_data[self.image_index])
    @observe('data')
    def _update_num_images(self, update):
        print("len(self.data): {}".format(len(self.sliceable_data)))
        # update the number of images
        self.num_images = len(self.sliceable_data)-1
        if self.image_index >= self.num_images:
            # this would be the case when a completely new image stack is thrown
            # at this model
            self.image_index = 0
        if self.auto_update:
            # move to the last image in the stack
            self.image_index = self.num_images
        # repaint the canvas with a new set of images
        self.cs.update_image(self.sliceable_data[self.image_index])
    @observe('cmap')
    def _update_cmap(self, update):
        self.cs.update_cmap(self.cmap)
    @observe('interpolation')
    def _update_interpolation(self, update):
        self.cs.update_interpolation(self.interpolation)
    @observe('limit_func')
    def _update_limit(self, update):
        self.cs.set_limit_func(self.limit_func)

    # NOT IMEPLEMENTED YET
    @observe('disp_min')
    def _update_min(self, update):
        # todo get image limits working
        pass
    @observe('disp_max')
    def _update_max(self, update):
        # todo get image limits working
        pass