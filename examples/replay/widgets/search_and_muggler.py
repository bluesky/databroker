
import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np

from databroker.muggler.api import DataMuggler
from databroker.replay.search import GetLastModel
from databroker.replay.muggler import MugglerModel
from databroker.replay.scalar import ScalarCollection

with enaml.imports():
    from search_and_muggler import MainView

app = QtApplication()

get_last_model = GetLastModel()

muggler_model = MugglerModel()

scalar_collection = ScalarCollection()

# set up observers
muggler_model.observe('data_muggler', scalar_collection.new_data_muggler)
muggler_model.new_data_callbacks.append(scalar_collection.notify_new_data)

# muggler_model.muggler = DataMuggler()

main_view = MainView(get_last_model=get_last_model,
                     muggler_model=muggler_model,
                     scalar_collection=scalar_collection)
print('scalar collection figure id: {}'.format(id(scalar_collection._fig)))
main_view.show()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
