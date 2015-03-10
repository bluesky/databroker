import six
if six.PY2:
    # hide enaml imports from python 3
    import enaml
    from enaml.qt.qt_application import QtApplication
    from dataportal.replay import replay

from functools import wraps
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore import api as mdsapi
from filestore.utils.testing import fs_setup, fs_teardown
from dataportal.examples.sample_data import temperature_ramp, image_and_scalar
from dataportal.broker import DataBroker as db
import copy
from ..testing.decorators import skip_if
import time as ttime
import os

global hdr_temp_ramp, ev_temp_ramp
global hdr_img_scalar, ev_img_scalar

import subprocess, threading

class Command(object):
    """Thanks SO! http://stackoverflow.com/a/4825933

    Example
    -------
    >>> command = Command("echo 'Process started'; sleep 2; echo 'Process finished'")
    >>> command.run(timeout=3)
        Thread started
        Process started
        Process finished
        Thread finished
        0
    >>> command.run(timeout=1)
        Thread started
        Process started
        Terminating process
        Thread finished
        -15
    """
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        def target():
            print('Thread started')
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()
            print('Thread finished')

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print('Terminating process')
            self.process.terminate()
            thread.join()
        print(self.process.returncode)

@skip_if(not six.PY2)
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


@skip_if(not six.PY2)
def teardown():
    fs_teardown()
    mds_teardown()


# these next two functions simply smoketest the startup of replay with
# different combinations of layout parameters. Whether or not anything
# actually works correctly is dealt with in later tests
@skip_if(not six.PY2)
def _replay_startup_tester(params=None, wait_time=1000):
    app = QtApplication()
    ui = replay.create(params)
    ui.show()
    app.timed_call(wait_time, app.stop)
    app.start()
    ui.close()
    app.destroy()


@skip_if(not six.PY2)
def test_replay_startup():
    normal = replay.define_default_params()
    small = replay.define_small_screen_params()
    live = replay.define_live_params()
    live_small = copy.deepcopy(live)
    live_small['screen_size'] = 'small'
    params = [normal, small, live, live_small]
    for p in params:
        yield _replay_startup_tester, p

# make sure that you can run dataportal/replay/replay.py
@skip_if(not six.PY2)
def test_replay_cmd_line():
    command = Command('python {}'.format(replay.__file__))
    command.run(timeout=1)


# make sure that you can run replay via the 'replay' command
@skip_if(not six.PY2)
def test_replay_cmd_line():
    command = Command('replay')
    command.run(timeout=1)


# this function tests that a live-view replay will correctly plot
# 'point_det' versus 'Tsam' when they are assigned to 'plotx' and 'ploty',
# respectively
@skip_if(not six.PY2)
def test_replay_plotting1():
    # insert a run header with one plotx and one ploty
    rs = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=1,
        custom={'plotx': 'Tsam', 'ploty': ['point_det']},
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    temperature_ramp.run(rs)
    # plotting replay in live mode with plotx and ploty should have the
    # following state after a few seconds of execution:
    # replay.
    app = QtApplication()
    ui = replay.create(replay.define_live_params())
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    # the x axis should be 'plotx'
    assert ui.scalar_collection.x == 'Tsam'
    # there should only be 1 scalar model currently plotting
    assert len([scalar_model for scalar_model
                in ui.scalar_collection.scalar_models.values()
                if scalar_model.is_plotting]) == 1
    # the x axis should not be time
    assert not ui.scalar_collection.x_is_time
    ui.close()
    app.destroy()


# this function tests that a live-view replay will correctly plot
# 'Tsam' versus time when plotx is incorrectly defined
@skip_if(not six.PY2)
def test_replay_plotting2():
    ploty = ['Tsam', 'point_det']
    plotx = 'this better fail!'
    # insert a run header with one plotx and one ploty
    rs = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=1,
        custom={'plotx': plotx, 'ploty': ploty},
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    temperature_ramp.run(rs)
    # plotting replay in live mode with plotx and ploty should have the
    # following state after a few seconds of execution:
    # replay.
    app = QtApplication()
    ui = replay.create(replay.define_live_params())
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    # there should only be 1 scalar model currently plotting
    assert len([scalar_model for scalar_model
                in ui.scalar_collection.scalar_models.values()
                if scalar_model.is_plotting]) == len(ploty)
    # the x axis should not be time
    assert ui.scalar_collection.x_is_time
    ui.close()
    app.destroy()


# this function tests that a live-view replay will correctly plot
# time on the x axis with none of the y values enabled for plotting if
# 'ploty' and 'plotx' are not found in the run header
@skip_if(not six.PY2)
def test_replay_plotting3():
    # insert a run header with one plotx and one ploty
    rs = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=1,
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    events = temperature_ramp.run(rs)
    # plotting replay in live mode with plotx and ploty should have the
    # following state after a few seconds of execution:
    # replay.
    app = QtApplication()
    ui = replay.create(replay.define_live_params())
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    # there should only be 1 scalar model currently plotting
    assert len([scalar_model for scalar_model
                in ui.scalar_collection.scalar_models.values()
                if scalar_model.is_plotting]) == 0
    # the x axis should not be time
    assert ui.scalar_collection.x_is_time
    ui.close()
    app.destroy()
