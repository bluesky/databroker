import enaml
from enaml.qt.qt_application import QtApplication
import numpy as np
from dataportal import replay
from dataportal.replay.search import (GetLastModel, WatchForHeadersModel,
                                      DisplayHeaderModel, ScanIDSearchModel)
from dataportal.broker import DataBroker as db
from dataportal.replay.muxer import MuxerModel
from dataportal.replay.scalar import ScalarCollection
import sys
from persist import History
import argparse
import os
import logging
import six
import errno
with enaml.imports():
    from dataportal.replay.replay_view import MainView

logger = logging.getLogger(__name__)

REPLAY_CONF_DIR = os.getenv('XDG_DATA_HOME')
if not REPLAY_CONF_DIR:
    REPLAY_CONF_DIR = os.path.join(os.path.expanduser('~'), '.local', 'share')


if six.PY2:
    # http://stackoverflow.com/a/5032238/380231
    def _make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
else:
    # technically, this won't work with py3.1, but no one uses that
    def _make_sure_path_exists(path):
        return os.makedirs(path, exist_ok=True)


_make_sure_path_exists(REPLAY_CONF_DIR)


STATE_DB = os.path.join(REPLAY_CONF_DIR, 'state.db')
history = History(STATE_DB)


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
    scan_id_model = ScanIDSearchModel(history)
    get_last_model = GetLastModel(history)
    muxer_model = MuxerModel()
    scalar_collection = ScalarCollection(history)
    display_header_model = DisplayHeaderModel()
    watch_headers_model = WatchForHeadersModel(history)

    watch_headers_model.auto_update = init_params_dict['automatically_update_header']

    if 'muxer_auto_update' in init_params_dict:
        muxer_model.auto_updating = init_params_dict['muxer_auto_update']

    # set up observers

    muxer_model.observe('dataframe', scalar_collection.new_dataframe)
    muxer_model.new_data_callbacks.append(scalar_collection.new_data)
    # muxer_model.observe('plot_state', scalar_collection.set_plot_state)
    muxer_model.observe('header_id', scalar_collection.dataframe_id_changed)

    watch_headers_model.observe('header', display_header_model.new_run_header)
    get_last_model.observe('header', display_header_model.new_run_header)
    scan_id_model.observe('header', display_header_model.new_run_header)
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
    parser.add_argument('--log', action="store", dest='log_level',
                        type=str, help="Set the replay logging level. Known "
                                       "options are 'debug', 'info', 'warning', "
                                       "'error', 'critical")
    parser.add_argument('--verbose', action="store_true",
                        help="Print extra information to the console. Sets the "
                             "logging level to 'info'")
    parser.add_argument('--debug', action="store_true",
                        help="Print way too much information to the console."
                             "Sets the logging level to'debug'")
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
    logger.info('args: {}'.format(args))
    if args.live:
        params_dict = define_live_params()
    elif args.small_screen:
        params_dict = define_small_screen_params()
    loglevel = logging.WARNING
    if args.verbose:
        loglevel = logging.INFO
    elif args.debug:
        loglevel = logging.DEBUG

    replay.logger.setLevel(loglevel)
    replay.handler.setLevel(loglevel)

    # create and show the GUI
    ui = create(params_dict)
    ui.show()

    # show the most recent run by default
    try:
        ui.muxer_model.header = db[-1]
    except IndexError:
        pass

    app.start()


if __name__ == "__main__":
    main()
