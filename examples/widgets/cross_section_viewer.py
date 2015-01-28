from enaml.qt.qt_application import QtApplication
import numpy as np

from replay.gui.api import make_cross_section_view


def change_data(data_lst):
    nx = np.random.randint(1000)+1
    ny = np.random.randint(1000)+1
    # model.data = data_lst
    model.data = [np.random.random((nx, ny)), ]

app = QtApplication()

model, view = make_cross_section_view()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
