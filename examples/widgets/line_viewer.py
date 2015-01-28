__author__ = 'edill'

from enaml.qt.qt_application import QtApplication
import numpy as np

from replay.gui.api import make_line_window


def change_data(model, time):
    x0 = np.random.random() * 2
    x1 = np.random.random() * 10
    x = np.arange(x0, x1, .01)
    y = np.sin(x) * np.random.random() * 2
    model.add_xy(x, y, time)

app = QtApplication()
model, view = make_line_window()
x = np.arange(0, 10, .01)
y = np.sin(x)
xy = {'init': (x.tolist(), y.tolist())}
model.set_xy(x.tolist(), y.tolist())
view.show()
step = 400
for ms in np.arange(500, 10000, step):
    app.timed_call(ms, change_data, model, ms)
app.start()
