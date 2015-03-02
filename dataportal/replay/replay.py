
import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np

from dataportal.muxer.api import DataMuxer
from dataportal.replay.search import (GetLastModel, WatchForHeadersModel,
                                      DisplayHeaderModel)
from dataportal.replay.muxer import MuxerModel
from dataportal.replay.scalar import ScalarCollection
import sys

import argparse

with enaml.imports():
    from dataportal.replay.replay_view import MainView


def define_default_params():
    """Default replay view. No auto updating of anything
    """
    params_dict = {
        'search_tab_index': 0,
        'automatically_update_header': False,
    }
    return params_dict


def define_live_params():
    """Stopgap for live data viewing
    """
    params_dict = {
        'search_tab_index': 1,
        'automatically_update_header': True,
        'muxer_auto_update': True,
    }
    return params_dict


def read_from_yaml(fname):
    """Read Replay parameters from a yaml config
    """
    import yaml
    with open(fname, 'r') as f:
        return yaml.load(f)


def create_default_ui(init_params_dict):
    get_last_model = GetLastModel()
    muxer_model = MuxerModel()
    scalar_collection = ScalarCollection()
    display_header_model = DisplayHeaderModel()
    watch_headers_model = WatchForHeadersModel()
    watch_headers_model.auto_update = init_params_dict['automatically_update_header']

    if 'muxer_auto_update' in init_params_dict:
        muxer_model.auto_updating = init_params_dict['muxer_auto_update']

    # set up observers


    muxer_model.observe('dataframe', scalar_collection.new_dataframe)
    # muxer_model.new_data_callbacks.append(scalar_collection.notify_new_data)

    watch_headers_model.observe('header', display_header_model.new_run_header)
    get_last_model.observe('header', display_header_model.new_run_header)
    display_header_model.observe('header', muxer_model.new_run_header)


    get_last_model.observe('selected', scalar_collection.header_changed)

    main_view = MainView(get_last_model=get_last_model, muxer_model=muxer_model,
                         scalar_collection=scalar_collection,
                         watch_headers_model=watch_headers_model,
                         display_header_model=display_header_model,
                         init_params=init_params_dict)
    return main_view

def define_parser():
    parser = argparse.ArgumentParser(description='Launch a data viewer')
    parser.add_argument('--live', action="store_true",
                        help="Launch Replay configured for viewing live data")
    # parser.add_argument('--conf',
    #                     help="Launch Replay based on configuration from "
    #                          "specified file")
    return parser

def main():
    parser = define_parser()
    args = parser.parse_args()
    if args.live:
        params_dict = define_live_params()
    else:
        params_dict = define_default_params()
    app = QtApplication()
    ui = create_default_ui(params_dict)
    ui.show()
    app.start()

if __name__ == "__main__":
    main()
