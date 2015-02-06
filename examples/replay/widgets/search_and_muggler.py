
import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np

from databroker.replay.search import GetLastModel, GetLastView
from databroker.replay.muggler import MugglerModel, MugglerController

with enaml.imports():
    from search_and_muggler import MainView

app = QtApplication()

get_last_model = GetLastModel()

muggler_model = MugglerModel()

main_view = MainView(get_last_model=get_last_model,
                     muggler_model=muggler_model)
main_view.show()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
