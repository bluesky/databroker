__author__ = 'edill'

from enaml.qt.qt_application import QtApplication
import numpy as np

from replay.gui.api import make_line_view


def change_data(model):
    x0 = np.random.random() * 2
    x1 = np.random.random() * 10
    x = np.arange(x0, x1, .01)
    y = np.sin(x)
    model.set_xy(x, y)

app = QtApplication()
model, view = make_line_view()
view.show()
for _ in np.arange(500, 10000, 500):
    app.timed_call(_, change_data, model)
app.start()
