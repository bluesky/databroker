__author__ = 'edill'
from replay.model.fitting_model import FitController
from skxray.fitting.model.physics_model import model_list
fc = FitController(model_list)
import numpy as np
x = np.arange(0, 10, .01)
y = x
fc.set_xy(x, y)
fc.current_model = 'LinearModel'
lmfit_model = fc.atom_models[fc.current_model].lmfit_model
params = lmfit_model.do_guess(y, x=x)
lmfit_model.fit(y, params, x=x)
fit = lmfit_model.fit(y, params, x=x)
print('fit values: {}'.format(fit.values))
