__author__ = 'edill'
import six
from atom.api import (Atom, List, observe, Bool, Enum, Str, Int, Range, Float,
                      Typed, Dict, Coerced)
import numpy as np
from matplotlib.figure import Figure
from matplotlib import colors
from bubblegum.backend.mpl.cross_section_2d import CrossSection
from skxray.spectroscopy import find_largest_peak
from matplotlib.lines import Line2D
from skxray.fitting.api import model_list
import six
import sys
from lmfit import Model, Parameter, Parameters
import logging
logger = logging.getLogger(__name__)

if model_list is None:
    from skxray.fitting.models import GaussianModel
    default_models = [GaussianModel]
else:
    default_models = model_list

class ParameterModel(Atom):
    """Atom version of the lm-fit Parameter class

    """
    disabled = 'off'
    name = Str()
    value = Float()
    min = Float()
    max = Float()
    stderr = Float()
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
        if param.min is not None:
            self.min = param.min
        if param.max is not None:
            self.max = param.max
        self.vary = param.vary
        # check for none
        # stderr = param.stderr
        # if stderr is None:
        #     stderr = 0
        # self.stderr = stderr

    @observe('init_value', 'min', 'max', 'vary', 'bounded_min', 'bounded_max',
             'min_bound_type', 'max_bound_type')
    def observe_changeables(self, changed):
        try:
            self.lmfit_param.set({changed['name']: changed['value']})
        except KeyError:
            # the parameter doesn't exist in the lmfit parameter
            pass
        # print(changed)


class FitModel(Atom):
    """Back-end for the FitController

    """
    params = Dict()
    lmfit_params = Typed(Parameters)
    lmfit_model = Typed(Model)
    name = Str()
    show_advanced = Bool(False)
    show_basic = Bool(True)

    def __init__(self, lmfit_model, prefix, name=None):
        # print(lmfit_model)
        if name is None:
            name = str(lmfit_model.name)
        self.name = name
        if type(lmfit_model) is type:
            self.lmfit_model = lmfit_model(name=self.name, prefix=prefix)
        else:
            self.lmfit_model = lmfit_model
        # print(self.lmfit_model.name)
        # self.lmfit_model.prefix = "1"
        self.lmfit_params = self.lmfit_model.make_params()
        for name, param in six.iteritems(self.lmfit_params):
            p = ParameterModel()
            p.set_from_parameter(param)
            self.params[name] = p
        super(FitModel, self).__init__()

    @observe('show_advanced')
    def update_view(self, changed):
        self.show_basic = not self.show_advanced
        print('show_basic: {}'.format(self.show_basic))
        print('show_advanced: {}'.format(self.show_advanced))

    def update_params(self, params):
        """
        Parameters
        ----------
        params : list
            List of parameters that the fit model knows about. Silently ignore
            any parameters that are passed in that this model doesn't know about
        """
        for param in params:
            try:
                self.params[param.name].set_from_parameter(param)
            except KeyError:
                # silently ignore keys that are not in this model
                pass

class PolynomialModel(FitModel):
    """Back-end specific to the polynomial model
    """
    polynomial_order = Int(2)
    def __init__(self, lmfit_model, prefix, poly_order=None, **kwargs):
        if poly_order is None:
            poly_order = self.polynomial_order
        self.polynomial_order = poly_order
        lmfit_model = lmfit_model(degree=self.polynomial_order, prefix=prefix)
        super(PolynomialModel, self).__init__(lmfit_model, prefix=prefix, **kwargs)

class FitController(Atom):
    """ Back-end for the enaml fitting viewer

    """

    atom_models = Dict()
    model_names = List()
    _valid_models = List()
    current_model = Str()
    show_params = Bool(True)


    def __init__(self, prefix, valid_models=None, name=None, model_name=None
                 ):
        # set some defaults
        if valid_models is None:
            valid_models = default_models
        self._valid_models = valid_models
        if model_name is None:
            model_name = 'GaussianModel'
        self.current_model = model_name
        # init the model chooser
        for model in self._valid_models:
            try:
                the_model = FitModel(model, name=name, prefix=prefix)
            except TypeError as te:
                if model.__name__ == 'PolynomialModel':
                    the_model = PolynomialModel(lmfit_model=model, name=name,
                                                prefix=prefix)
                else:
                    six.reraise(TypeError, te, sys.exc_info()[2])
            self.atom_models[model.__name__] = the_model
        self.model_names = list(self.atom_models)
        self.model_names.sort()

    def lmfit_model(self):
        return self.get_current_model().lmfit_model

    def get_current_model(self):
        return self.atom_models[self.current_model]


class MultiFitController(Atom):
    models = List(item=FitController)
    valid_models = List(item=type)
    guess = Bool(True)
    autofit = Bool(False)
    x = Typed(np.ndarray)
    y = Typed(np.ndarray)
    fit_artist = Typed(Line2D)
    best_fit = Coerced(list)

    def __init__(self, valid_models=None):
        if valid_models  is None:
            valid_models = default_models
        self.valid_models = valid_models
        gaussian = FitController(model_name='GaussianModel', prefix="a_")
        linear = FitController(model_name="LinearModel", prefix="b_")
        self.models = [gaussian, linear, ]

    def add_model(self):
        models = self.models
        fc = FitController()

        models.append(FitController(name = str(len(self.models))))
        self.models = models
        # self.models.append(FitController())

    def set_xy(self, x, y):
        print('set_xy, x: {}'.format(x))
        print('set_xy, y: {}'.format(y))
        x = np.asarray(x)
        y = np.asarray(y)
        self.x = x
        self.y = y

    def update_params(self, params):
        print('in update_params. params: {}'.format(params))
        for model in self.models:
            model.get_current_model().update_params(six.itervalues(params))
    def aggregate_models(self):
        if len(self.models) == 0:
            return None
        aggregated = self.models[0].lmfit_model()
        if len(self.models) == 1:
            return aggregated
        for model in self.models[1:]:
            aggregated += model.lmfit_model()
        return aggregated

    def do_guess(self):
        if self.x is None or len(self.x) == 0:
            return
        pars = None
        for model in self.models:
            if pars is None:
                pars = model.lmfit_model().guess(self.y, x=self.x)
            else:
                guessed = model.lmfit_model().guess(self.y, x=self.x)
                pars += guessed
        # pars = list(six.itervalues(pars))
        print('guessed parameters: {}'.format(pars))
        self.update_params(pars)
        aggregated_models = self.aggregate_models()
        self.best_fit = aggregated_models.eval(params=pars, data=self.y, x=self.x)
        return pars

    def fit(self):
        pars = self.do_guess()
        aggregated_models = self.aggregate_models()
        fit_res = aggregated_models.fit(params=pars, data=self.y, x=self.x)
        self.best_fit = fit_res.best_fit
        # print(dir(fit_res))
        self.update_params(fit_res.params)