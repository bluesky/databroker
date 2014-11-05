__author__ = 'edill'
import six
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Coerced)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from nsls2.spectroscopy import find_largest_peak
from matplotlib.lines import Line2D
import six
import sys
from lmfit import Model, Parameter, Parameters
import logging
logger = logging.getLogger(__name__)


class ParameterModel(Atom):
    """Atom version of the lm-fit Parameter class

    """
    disabled = 'off'
    name = Str()
    value = Float()
    min = Float()
    max = Float()
    vary = Bool(True)
    bounded_min = Bool(False)
    bounded_max = Bool(False)
    min_bound_type = Enum(disabled, '<', '<=')
    max_bound_type = Enum(disabled, '<', '<=', '=')
    lmfit_param = Typed(Parameter)

    def set_from_parameter(self, param):
        self.lmfit_param = param
        self.name = param.name
        if param.value is None:
            param.value = 0
        self.value = param.value
        self.min = param.min
        self.max = param.max
        self.vary = param.vary

    @observe('init_value', 'min', 'max', 'vary', 'bounded_min', 'bounded_max',
             'min_bound_type', 'max_bound_type')
    def observe_changeables(self, changed):
        try:
            self.lmfit_param.set({changed['name']: changed['value']})
        except KeyError:
            # the parameter doesn't exist in the lmfit parameter
            pass
        print(changed)


class FitModel(Atom):
    """Back-end for the FitController

    """
    params = Dict()
    lmfit_params = Typed(Parameters)
    lmfit_model = Typed(Model)
    name = Str()

    def __init__(self, lmfit_model, *args, **kwargs):
        print(lmfit_model)
        if type(lmfit_model) is type:
            self.lmfit_model = lmfit_model()
        else:
            self.lmfit_model = lmfit_model
        print(self.lmfit_model.name)
        self.lmfit_params = self.lmfit_model.make_params()
        for name, param in six.iteritems(self.lmfit_params):
            p = ParameterModel()
            p.set_from_parameter(param)
            self.params[name] = p
        name = lmfit_model.name
        super(FitModel, self).__init__()

    def update_params(self, params):
        """
        Parameters
        ----------
        params : list
            List of parameters that the fit model knows about
        """
        for param in params:
            self.params[param.name].set_from_parameter(param)

class PolynomialModel(FitModel):
    """Back-end specific to the polynomial model
    """
    polynomial_order = Int(2)
    def __init__(self, lmfit_model, poly_order=None):
        if poly_order is None:
            poly_order = self.polynomial_order
        self.polynomial_order = poly_order
        lmfit_model = lmfit_model(degree=self.polynomial_order)
        super(PolynomialModel, self).__init__(lmfit_model)

class FitController(Atom):
    """ Back-end for the enaml fitting viewer

    """

    atom_models = Dict()
    model_names = List()
    _valid_models = List()
    current_model = Str()
    autofit = Bool(False)
    x = Typed(np.ndarray)
    y = Typed(np.ndarray)
    best_fit = Coerced(list)
    fit_artist = Typed(Line2D)
    guess = Bool(True)


    def __init__(self, valid_models):
        self._valid_models = valid_models
        for model in self._valid_models:
            try:
                the_model = FitModel(model)
            except TypeError as te:
                if model.__name__ == 'PolynomialModel':
                    the_model = PolynomialModel(lmfit_model=model)
                else:
                    six.reraise(TypeError, te, sys.exc_info()[2])
            self.atom_models[model.__name__] = the_model
            self.current_model = 'GaussianModel'
        self.model_names = list(self.atom_models)
        self.model_names.sort()

    def set_xy(self, x, y):
        print('set_xy, x: {}'.format(x))
        print('set_xy, y: {}'.format(y))
        x = np.asarray(x)
        y = np.asarray(y)
        self.x = x
        self.y = y

    def do_guess(self):
        atom_model = self.atom_models[self.current_model]
        try:
            pars = atom_model.lmfit_model.guess(self.y, x=self.x)
            pars = list(six.itervalues(pars))
            print('guessed parameters: {}'.format(pars))
            atom_model.update_params(pars)
        except AttributeError:
            pass
        # except NotImplementedError as nie:
        #     if self.current_model == 'GaussianModel':
        #         if len(self.x) == 0:
        #             return
        #         window = 1000
        #         if window < len(self.x):
        #             window = len(self.x)
        #         params = find_largest_peak(self.x, self.y)
        #         self.atom_models[self.current_model].params['center'].value = params[0]
        #         self.atom_models[self.current_model].params['amplitude'].value = params[1]
        #         self.atom_models[self.current_model].params['sigma'].value = params[2]
        #
        #     else:
        #         # do nothing
        #         print('lmfit model guess failed: {}'.format(nie))

    def fit(self):
        atom_model = self.atom_models[self.current_model]
        print('atom_model.lmfit_params: {}'.format(atom_model.lmfit_params))
        pars = atom_model.lmfit_model.guess(self.y, x=self.x)
        out = atom_model.lmfit_model.fit(self.y,
                                         params=pars,
                                         x=self.x)
        for var_name, var_value in six.iteritems(out.values):
            atom_model.params[var_name].value = var_value
        self.best_fit = out.best_fit
        # print('dir: {}'.format(dir(out)))
        print('out.values: {}'.format(out.values))
        # print('out.params: {}'.format(out.params))
        # print('out: {}'.format(out))


