__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from lmfit import Model
import logging
logger = logging.getLogger(__name__)

class FittingModel(Atom):
    """ Back-end for the enaml fitting viewer

    """

    # PARAMETERS -- VIEWER
    _fit_x = List()
    _fit_y = List()

    model = Typed(Model)
    p_dict = Dict()

    def __init__(self, fit_model):
        self.model = fit_model
        self.p_dict = self.model().make_params()

