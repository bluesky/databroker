__author__ = 'edill'

from enaml.qt.qt_application import QtApplication
import numpy as np

from replay.gui.api import make_image_window


def change_data(model):
    nx = np.random.randint(1000)
    ny = np.random.randint(1000)
    model.data = np.random.random((nx, ny))

app = QtApplication()
model, view = make_image_window()
view.show()
step = 400
for ms in np.arange(500, 10000, step):
    app.timed_call(ms, change_data, model)
app.start()
