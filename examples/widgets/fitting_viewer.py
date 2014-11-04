from __future__ import unicode_literals, print_function, absolute_import
import six
from enaml.qt.qt_application import QtApplication
import numpy as np

import numpy as np
import enaml
from replay.model.fitting_model import ParameterModel
__author__ = 'edill'

app = QtApplication()

with enaml.imports():
    from replay.gui.fitting_view import ParameterMain

p = ParameterModel(name='x', init_value=10, min=1, max=15, vary=False)
view = ParameterMain(param=p)
view.show()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
