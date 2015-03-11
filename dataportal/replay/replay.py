import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np
from dataportal.muxer.api import DataMuxer
from dataportal.replay.search import (GetLastModel, WatchForHeadersModel,
                                      DisplayHeaderModel, ScanIDSearchModel)
from dataportal.replay.muxer import MuxerModel
from dataportal.replay.scalar import ScalarCollection
import sys
from persist import History
import argparse
import os
with enaml.imports():
    from dataportal.replay.replay_view import MainView


REPLAY_CONF_DIR = os.path.join(os.path.expanduser('~'), '.config', 'replay')
try:
    os.mkdir(REPLAY_CONF_DIR)
except OSError:
    # path already exists
    pass

STATE_DB = os.path.join(REPLAY_CONF_DIR, 'state.db')
history = History(STATE_DB)
REPLAY_HISTORY_KEYS = ['x', 'y_list', 'x_is_time']


def define_default_params():
    """Default replay view. No auto updating of anything
    """
    params_dict = {
        'search_tab_index': 0,
        'automatically_update_header': False,
        'screen_size': 'normal',
    }
    return params_dict


def define_live_params():
    """Stopgap for live data viewing
    """
    params_dict = {
        'search_tab_index': 1,
        'automatically_update_header': True,
        'muxer_auto_update': True,
        'screen_size': 'normal'
    }
    return params_dict


def define_small_screen_params():
    params_dict = define_default_params()
    params_dict['screen_size'] = 'small'
    return params_dict


def create_default_ui(init_params_dict):
    scan_id_model = ScanIDSearchModel()
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
    muxer_model.new_data_callbacks.append(scalar_collection.new_data)
    muxer_model.observe('plot_state', scalar_collection.set_plot_state)

    watch_headers_model.observe('header', display_header_model.new_run_header)
    get_last_model.observe('header', display_header_model.new_run_header)
    # scan_id_model.observe('header', display_header_model.new_run_header)
    display_header_model.observe('header', muxer_model.new_run_header)

    main_view = MainView(get_last_model=get_last_model, muxer_model=muxer_model,
                         scalar_collection=scalar_collection,
                         watch_headers_model=watch_headers_model,
                         display_header_model=display_header_model,
                         init_params=init_params_dict,
                         scan_id_model=scan_id_model)
    return main_view

def define_parser():
    parser = argparse.ArgumentParser(description='Launch a data viewer')
    parser.add_argument('--live', action="store_true",
                        help="Launch Replay configured for viewing live data")
    parser.add_argument('--small-screen', action="store_true",
                        help="Launch Replay configured for viewing data on a "
                             "small screen. Tested as low as 1366x768")
    return parser

def create(params_dict=None):
    if params_dict is None:
        params_dict = define_default_params()
    return create_default_ui(params_dict)


def main():
    app = QtApplication()
    parser = define_parser()
    args = parser.parse_args()
    params_dict = None
    print('args: {}'.format(args))
    if args.live:
        params_dict = define_live_params()
    elif args.small_screen:
        params_dict = define_small_screen_params()

    # create and show the GUI
    create(params_dict).show()

    app.start()


if __name__ == "__main__":
    main()
