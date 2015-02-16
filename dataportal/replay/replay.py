
import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np

from dataportal.muxer.api import DataMuxer
from dataportal.replay.search import GetLastModel
from dataportal.replay.muxer import MuxerModel
from dataportal.replay.scalar import ScalarCollection

with enaml.imports():
    from dataportal.replay.replay_view import MainView


def create_ui():
    get_last_model = GetLastModel()

    muxer_model = MuxerModel()

    scalar_collection = ScalarCollection()

    # set up observers
    muxer_model.observe('data_muxer', scalar_collection.new_data_muxer)
    muxer_model.new_data_callbacks.append(scalar_collection.notify_new_data)


    main_view = MainView(get_last_model=get_last_model, muxer_model=muxer_model,
                         scalar_collection=scalar_collection)
    return main_view

def main():
    app = QtApplication()
    ui = create_ui()
    ui.show()
    app.start()

if __name__ == "__main__":
    main()
