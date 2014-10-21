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

class HistogramModel(Atom):
    # PARAMETERS -- VIEWER
    # x-axis
    bins = List()
    # y-axis
    values = List()

    # plot style
    # style = Enum('Bar', 'Scatter', 'Line')

    figure = Figure()
    _ax = figure.add_axes(111)
