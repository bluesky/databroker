__author__ = 'edill'

from replay.api import make_image_view
from enaml.qt.qt_application import QtApplication
import numpy as np

def change_data(model):
    nx = np.random.randint(1000)
    ny = np.random.randint(1000)
    model.data = np.random.random((nx, ny))

app = QtApplication()
model, view = make_image_view()
view.show()
for _ in np.arange(500, 10000, 500):
    app.timed_call(_, change_data, model)
app.start()
