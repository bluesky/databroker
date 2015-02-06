from enaml.qt.qt_application import QtApplication
import numpy as np

from databroker.replay.search import GetLastModel, GetLastWindow

app = QtApplication()

get_last_model = GetLastModel()
view = GetLastWindow(get_last_model=get_last_model)

view.show()

app.start()
