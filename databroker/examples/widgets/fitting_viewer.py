from __future__ import unicode_literals, print_function, absolute_import
import six
from enaml.qt.qt_application import QtApplication
import numpy as np
from skxray.fitting.model.physics_model import model_list
import numpy as np
import enaml
from replay.model.fitting_model import (ParameterModel, FitModel,
                                        FitController, MultiFitController)
__author__ = 'edill'

app = QtApplication()

with enaml.imports():
    from replay.gui.fitting_view import (ParameterMain, ModelMain,
                                         FitControllerMain,
                                         MultiFitControllerMain)

# p = ParameterModel(name='x', init_value=10, min=1, max=15, vary=False)
m = FitModel(model_list[0])
# param_view = ParameterMain(param=p)
fit_controller = FitController(model_list)
mfc = MultiFitController()
model_view = FitControllerMain(fit_controller=fit_controller)
mfc_view = MultiFitControllerMain(multi_fit_controller=mfc)
# model_view.show()
mfc_view.show()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
