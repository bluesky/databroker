from __future__ import (print_function, absolute_import, division)
import six
from pims import FramesSequence
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict)
import numpy as np
import sys
from bubblegum.backend.mpl.cross_section_2d import (CrossSection,
                                                    fullrange_limit_factory,
                                                    absolute_limit_factory,
                                                    percentile_limit_factory)
from ..pipeline.pipeline import DataMuggler, DmImgSequence
from datetime import datetime
from matplotlib.figure import Figure
from matplotlib import colors
from .histogram_model import HistogramModel

# create the colormap list
from matplotlib.cm import datad
mpl_colors = datad.keys()
mpl_colors.sort()
mpl_colors.pop(mpl_colors.index('jet'))
mpl_colors.pop(mpl_colors.index('jet_r'))

interpolation = ['none', 'nearest', 'bilinear', 'bicubic','spline16',
                 'spline36', 'hanning', 'hamming', 'hermite', 'kaiser',
                 'quadric', 'catrom', 'gaussian', 'bessel', 'mitchell',
                 'sinc', 'lanczos']

import logging
logger = logging.getLogger(__name__)

__author__ = 'edill'


class CrossSectionModel(Atom):
    """ Back-end for the Cross Section viewer and its control panel

    """
    redraw_type = Enum('s', 'frames')

    # PARAMETERS -- VIEWER
    # List of 2-D images
    sliceable_data = Typed(DmImgSequence)
    # interpolation routine to use
    interpolation = Enum(*interpolation)
    # color map to use
    cmap = Enum(*mpl_colors)
    # Matplotlib figure to draw the cross section on
    _fig = Typed(Figure)
    # figure = Figure()
    # normalization routine to use
    norm = Enum([colors.Normalize, colors.LogNorm])
    # name of limit function to use
    limit_func_type = Enum('full range', 'percentile', 'absolute')
    # actual limit function
    limit_func = Typed(object)

    # back end for plotting. cs holds a figure that paints the cross section
    # viewer
    cs = Typed(CrossSection)
    # name of the data set that this CrossSectionModel represents
    name = Str()
    # data muggler
    dm = Typed(DataMuggler)
    dm_names = List()
    visible = Bool(True)

    # histogram model
    histogram_model = Typed(HistogramModel)

    # PARAMETERS -- AUTOSCALING
    autoscale_horizontal = Bool(True)
    autoscale_vertical = Bool(True)

    # PARAMETERS -- CONTROL DOCK
    # minimum value for the slider
    minimum = Int(0)
    # maximum value for the slider
    num_images = Int()
    # slider value
    image_index = Int(0)
    # auto-update image
    auto_update = Bool(True)

    # UPDATE SPEED CONTROL
    redraw_every = Float(default=1)
    redraw_type = Enum('max rate', 's', 'frames')
    last_update_time = Typed(datetime)
    last_update_frame = Int()
    update_rate = Str()
    num_updates = Int()

    # absolute minimum of the  currently selected image
    img_min = Float()
    # absolute minimum of the currently selected image
    img_max = Float()
    disp_min_percentile = Float()
    disp_max_percentile = Float()
    disp_min_absolute = Float()
    disp_max_absolute = Float()
    min_enabled = Bool(True)
    max_enabled = Bool(True)
    disp_min = Float()
    # currently displayed maximum of the currently selected image
    disp_max = Float()

    def __init__(self, data_muggler, sliceable_data=None, name=None,
                 histogram_model=histogram_model):
        with self.suppress_notifications():
            # stash the data muggler
            self.dm = data_muggler
            # grab the valid 2-d names from the data muggler
            self.dm_names = self.dm.keys(dim=2)
            # set data muggler name default
            if name is None:
                try:
                    name = self.dm_names[0]
                except IndexError:
                    name = None
            # stash the name
            self.name = name
            # set defaults for sliceable data
            if sliceable_data is None:
                sliceable_data = DmImgSequence(data_muggler=self.dm,
                                               data_name=self.name)
            # create the mpl figure
            self._fig = Figure()
            # set the default color map
            cmap = 'BuPu_r'
            # init the limit function
            self.limit_func_type = 'percentile'
            self.disp_min = 1
            self.disp_max = 99
            self.disp_min_percentile = self.disp_min
            self.disp_max_percentile = self.disp_max
            self.limit_func = percentile_limit_factory([self.disp_min, self.disp_max])
            self.cs = CrossSection(fig=self._fig, cmap=cmap,
                                   limit_func=self.limit_func)
            # grab the color map and interpolation from the cross section mpl
            # object
            self.cmap = self.cs._cmap
            self.interpolation = self.cs._interpolation
            # hook up to the data muggler's 'I have new data' signal
            self.dm.new_data.connect(self.notify_new_data)
            if histogram_model is not None:
                self.histogram_model = histogram_model
                self.histogram_model.limit_func = self.limit_func

        self.img_max = 100
        self.img_min = 0
        self.sliceable_data = sliceable_data
        self.redraw_type = 's'

    # slot to hook the data muggler into
    def notify_new_data(self, new_data):
        self.num_updates += 1
        if self.name in new_data:
            self.num_images = len(self.sliceable_data) - 1
            redraw = False
            if self.redraw_type == 's':
                if ((datetime.utcnow() - self.last_update_time).total_seconds()
                        >= self.redraw_every):
                    redraw = True
                else:
                    # do nothing
                    pass
            elif self.redraw_type == 'frames':
                if ((self.num_images - self.last_update_frame)
                        >= self.redraw_every):
                    redraw = True
                    self.last_update_frame = self.num_images-1
                else:
                    # do nothing
                    pass

            elif self.redraw_type == 'max rate':
                redraw = True
            if self.auto_update and redraw:
                self.image_index = self.num_images-1

            if redraw:
                # process the update timer
                self.update_rate = "{0:.2f} s<sup>-1</sup>".format(float(
                    self.num_updates) / (
                    datetime.utcnow() - self.last_update_time).total_seconds())
                self.last_update_frame = self.num_images-1
                self.last_update_time = datetime.utcnow()
                self.num_updates = 0

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
                state += ("\n{}: attribute not available because an exception "
                         "was raised: {}".format(var, e))
        return state

    def recompute_image_stats(self):
        """ Function that computes image stats when the image is updated
        """
        img = self.sliceable_data[self.image_index]
        if self.limit_func_type == 'percentile':
            img_min = 0
            img_max = 100
        else:
            img_min = np.min(img)
            img_max = np.max(img)

        self.img_min = img_min
        self.img_max = img_max

    # OBSERVATION METHODS
    @observe('redraw_type')
    def _update_redraw_type(self, update):
        self.last_update_time = datetime.utcnow()
        self.last_update_frame = self.image_index
        print('self.redraw_type: {}'.format(self.redraw_type))

    @observe('redraw_every')
    def _update_redraw_delay(self, update):
        print('self.redraw_every: {}'.format(self.redraw_every))

    @observe('sliceable_data')
    def _update_sliceable_data(self, update):
        print('sliceable data updated')
        try:
            self.num_images = len(self.sliceable_data) - 1
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
        self.sliceable_data = DmImgSequence(data_muggler=self.dm,
                                            data_name=self.name)
        self.image_index = 0

    @observe('image_index')
    def _update_image(self, update):
        if self.image_index < 0:
            return
        print('self.image_index: {}'.format(self.image_index))
        print('self.image_shape: {}'.format(
            self.sliceable_data[self.image_index].shape))
        self.cs.update_image(self.sliceable_data[self.image_index])
        if self.histogram_model is not None:
            self.histogram_model.img = self.sliceable_data[self.image_index]
        self.recompute_image_stats()

    @observe('cmap')
    def _update_cmap(self, update):
        print('cmap: {}'.format(self.cmap))
        self.cs.update_cmap(self.cmap)
        self.histogram_model.cmap = self.cmap

    @observe('interpolation')
    def _update_interpolation(self, update):
        self.cs.update_interpolation(self.interpolation)

    @observe('disp_min', 'disp_max')
    def _update_disp_lims(self, update):
        print("update: {}".format(update))
        if self.limit_func_type == 'full range':
            pass
        elif self.limit_func_type == 'percentile':
            self.disp_min_percentile = self.disp_min
            self.disp_max_percentile = self.disp_max
            self.limit_func = percentile_limit_factory(
                limit_args=[self.disp_min, self.disp_max])
        elif self.limit_func_type == 'absolute':
            self.disp_min_absolute = self.disp_min
            self.disp_max_absolute = self.disp_max
            self.limit_func = absolute_limit_factory(
                limit_args=[self.disp_min, self.disp_max])
        self.cs.update_limit_func(self.limit_func)
        self.histogram_model.limit_func = self.limit_func

    @observe('limit_func_type')
    def _update_limit(self, update):
        img = self.sliceable_data[self.image_index]
        print('limits changed (func, min, max): {}, {}, {}'
              ''.format(self.limit_func_type, self.disp_min, self.disp_max))
        min_enabled = True
        max_enabled = True
        print("update: {}".format(update))
        if update['value'] == 'full range':
            min_enabled = False
            max_enabled = False
            self.limit_func = fullrange_limit_factory()
        elif update['value'] == 'percentile':
            self.img_min = 0
            self.img_max = 100
            self.disp_min = self.disp_min_percentile
            self.disp_max = self.disp_max_percentile
            self.limit_func = percentile_limit_factory(
                limit_args=[self.disp_min, self.disp_max])
        elif update['value'] == 'absolute':
            self.img_min = np.min(img)
            self.img_max = np.max(img)
            self.disp_min = self.disp_min_absolute
            self.disp_max = self.disp_max_absolute
            self.limit_func = absolute_limit_factory(
                limit_args=[self.disp_min, self.disp_max])

        self.min_enabled = min_enabled
        self.max_enabled = max_enabled
        self.cs.update_limit_func(self.limit_func)

    # TODO fix this @tacaswell :)
    @observe('autoscale_horizontal')
    def _update_horizontal_autoscaling(self, update):
        self.cs.autoscale_horizontal(self.autoscale_horizontal)

    @observe('autoscale_vertical')
    def _update_vertical_autoscaling(self, update):
        self.cs.autoscale_vertical(self.autoscale_vertical)


class CrossSectionCollection(Atom):
    cs_models = Dict(key=Str(), value=CrossSectionModel)
    data_muggler = Typed(DataMuggler)

    def __init__(self, data_muggler):
        super(CrossSectionCollection, self).__init__()
        self.data_muggler = data_muggler
        image_cols = [name for name, dim
                      in six.iteritems(self.data_muggler.col_dims)
                      if dim == 2]




