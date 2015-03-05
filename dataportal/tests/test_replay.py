import enaml
from enaml.qt.qt_application import QtApplication

import sys
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
from dataportal.examples.sample_data import temperature_ramp, image_and_scalar
from dataportal.broker import DataBroker as db
from dataportal.muxer import DataMuxer

from dataportal.replay import replay

global hdr_temp_ramp, ev_temp_ramp
global hdr_img_scalar, ev_img_scalar
global app
def setup():
    mds_setup()
    fs_setup()
    # set up the temperature ramp events
    global hdr_temp_ramp, ev_temp_ramp
    temperature_ramp.run()
    hdr_temp_ramp = db[-1]
    ev_temp_ramp = db.fetch_events(hdr_temp_ramp)
    global hdr_img_scalar, ev_img_scalar
    image_and_scalar.run()
    hdr_img_scalar = db[-1]
    ev_img_scalar = db.fetch_events(hdr_img_scalar)
    global app
    app = QtApplication()


def teardown():
    fs_teardown()
    mds_teardown()


def test_replay_normal_startup():
    replay.create_and_show()
    app.timed_call(1000, app.stop)
    app.start()


def test_replay_live_startup():
    replay.create_and_show(replay.define_default_params())
    app.timed_call(1000, app.stop)
    app.start()
