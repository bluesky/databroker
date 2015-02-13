
import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np

from dataportal.muxer.api import DataMuxer
from dataportal.replay.search import GetLastModel
from dataportal.replay.muxer import MuxerModel
from dataportal.replay.scalar import ScalarCollection

with enaml.imports():
    from search_and_muxer import MainView

app = QtApplication()

get_last_model = GetLastModel()

muxer_model = MuxerModel()

scalar_collection = ScalarCollection()

# set up observers
muxer_model.observe('data_muxer', scalar_collection.new_data_muxer)
muxer_model.new_data_callbacks.append(scalar_collection.notify_new_data)

# muxer_model.muxer = DataMuxer()

main_view = MainView(get_last_model=get_last_model,
                     muxer_model=muxer_model,
                     scalar_collection=scalar_collection)
print('scalar collection figure id: {}'.format(id(scalar_collection._fig)))
main_view.show()
# for _ in np.arange(500, 10000, 500):
#     app.timed_call(_, change_data, model)
app.start()
