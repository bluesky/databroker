__author__ = 'edill'

from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
import six
import sys
from lmfit import Model
import logging
logger = logging.getLogger(__name__)


class Parameter(Atom):
    """Atom version of the lm-fit Parameter class

    """
    name = Str()
    user_value = Float()
    init_value = Float()
    min = Float()
    max = Float()
    vary

class FitModel(Atom):
    """Back-end for the FitController

    """
    params = Dict()
    model = Typed(Model)

    def __init__(self, model, *args, **kwargs):
        print(model)
        if type(model) is type:
            self.model = model()
        else:
            self.model = model
        print(self.model.name)
        self.params.update(self.model.make_params())
        super(FitModel, self).__init__()

class PolynomialModel(FitModel):
    """Back-end specific to the polynomial model
    """
    polynomial_order = Int(2)
    def __init__(self, model, poly_order=None):
        if poly_order is None:
            poly_order = self.polynomial_order
        self.polynomial_order = poly_order
        model = model(degree=self.polynomial_order)
        super(PolynomialModel, self).__init__(model)

class FitController(Atom):
    """ Back-end for the enaml fitting viewer

    """

    models = Dict()
    model_names = List()
    _valid_models = List()
    current_model = Enum(_valid_models)


    def __init__(self, valid_models):
        self._valid_models = valid_models
        for model in self._valid_models:
            try:
                the_model = FitModel(model)
            except TypeError as te:
                if model.__name__ == 'PolynomialModel':
                    the_model = PolynomialModel(model=model)
                else:
                    six.reraise(TypeError, te, sys.exc_info()[2])
            self.models[model.__name__] = the_model
        self.model_names = list(self.models)
        self.model_names.sort()
