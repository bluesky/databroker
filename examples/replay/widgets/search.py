from enaml.qt.qt_application import QtApplication
import numpy as np

from databroker.replay.search import LastModel, LastView

app = QtApplication()

model = LastModel()
view = LastView(model=model)

view.show()

# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
