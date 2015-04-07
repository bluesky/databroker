import six
if six.PY2:
    # hide enaml imports from python 3
    import enaml
    from enaml.qt.qt_application import QtApplication
    from dataportal.replay import replay

from functools import wraps
from nose.tools import raises
from metadatastore.utils.testing import mds_setup, mds_teardown
from metadatastore import api as mdsapi
from filestore.utils.testing import fs_setup, fs_teardown
from dataportal.examples.sample_data import temperature_ramp, image_and_scalar
from dataportal.broker import DataBroker as db
import copy
from ..testing.decorators import skip_if
from ..testing.utils import Command
import time as ttime
import os
import random
import tempfile
import uuid
from dataportal.replay.persist import History

global hdr_temp_ramp, ev_temp_ramp
global hdr_img_scalar, ev_img_scalar


@skip_if(six.PY3)
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


@skip_if(six.PY3)
def teardown():
    fs_teardown()
    mds_teardown()


# these next two functions simply smoketest the startup of replay with
# different combinations of layout parameters. Whether or not anything
# actually works correctly is dealt with in later tests
@skip_if(six.PY3)
def _replay_startup_tester(params, title, wait_time=1000):
    app = QtApplication()
    ui = replay.create(params)
    ui.title = title
    ui.show()
    app.timed_call(wait_time, app.stop)
    app.start()
    ui.close()
    app.destroy()


@skip_if(six.PY3)
def test_replay_startup():
    normal = replay.define_default_params()
    small = replay.define_small_screen_params()
    live = replay.define_live_params()
    live_small = copy.deepcopy(live)
    live_small['screen_size'] = 'small'
    params = [(normal, 'testing normal startup'),
              (small, 'testing small startup'),
              (live, 'testing live startup'),
              (live_small, 'testing small live startup')]
    for p in params:
        yield _replay_startup_tester, p[0], p[1]

# make sure that you can run dataportal/replay/replay.py
@skip_if(six.PY3)
def test_replay_cmd_line():
    command = Command('python {}'.format(replay.__file__))
    command.run(timeout=1)


# make sure that you can run replay via the 'replay' command
@skip_if(six.PY3)
def test_replay_cmd_line():
    command = Command('replay')
    command.run(timeout=1)


# this function tests that a live-view replay will correctly plot
# 'point_det' versus 'Tsam' when they are assigned to 'plotx' and 'ploty',
# respectively
@skip_if(six.PY3)
# these now raise because we got rid of plotx and ploty for now
@raises(AssertionError)
def test_replay_plotx_ploty():
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
    ui.title = 'testing replay with plotx and one value of ploty'
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    try:
        # the x axis should be 'plotx'
        assert ui.scalar_collection.x == 'Tsam'
        # there should only be 1 scalar model currently plotting
        assert len([scalar_model for scalar_model
                    in ui.scalar_collection.scalar_models.values()
                    if scalar_model.is_plotting]) == 1
        # the x axis should not be the index
        assert not ui.scalar_collection.x_is_index
    except AssertionError:
        # gotta destroy the app or it will cause cascading errors
        ui.close()
        app.destroy()
        raise

    ui.close()
    app.destroy()


# this function tests that a live-view replay will correctly plot
# 'Tsam' versus time when plotx is incorrectly defined
@skip_if(six.PY3)
# these now raise because we got rid of plotx and ploty for now
@raises(AssertionError)
def test_replay_plotx_2ploty():
    ploty = ['Tsam', 'point_det']
    plotx = 'this better fail!'
    # insert a run header with one plotx and two plotys
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
    ui.title = 'testing replay with plotx and two values of ploty'
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    try:
        # there should only be 1 scalar model currently plotting
        assert len([scalar_model for scalar_model
                    in ui.scalar_collection.scalar_models.values()
                    if scalar_model.is_plotting]) == len(ploty)
        # the x axis should not be the index
        assert ui.scalar_collection.x_is_index
    except AssertionError:
        # gotta destroy the app or it will cause cascading errors
        ui.close()
        app.destroy()
        raise
    ui.close()
    app.destroy()


# this function tests that a live-view replay will correctly plot
# the index on the x axis with none of the y values enabled for plotting if
# 'ploty' and 'plotx' are not found in the run header
@skip_if(six.PY3)
def test_replay_plotting():
    # insert a run header with no plotx or ploty
    rs = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=1,
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    events = temperature_ramp.run(rs)
    # plotting replay in live mode with plotx and ploty should have the
    # following state after a few seconds of execution:
    # replay.
    app = QtApplication()
    ui = replay.create(replay.define_live_params())
    ui.title = 'testing replay with no plotx and no ploty'
    ui.show()
    app.timed_call(4000, app.stop)
    app.start()
    try:
        # there should only be 1 scalar model currently plotting
        assert len([scalar_model for scalar_model
                    in ui.scalar_collection.scalar_models.values()
                    if scalar_model.is_plotting]) == 0
        # the x axis should not be the index
        assert ui.scalar_collection.x_is_index
    except AssertionError:
        # gotta destroy the app or it will cause cascading errors
        ui.close()
        app.destroy()
        raise
    ui.close()
    app.destroy()


# testing if replay is persisting state through closing correctly
@skip_if(six.PY3)
def test_replay_persistence():
    rs1 = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=1,
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    events1 = temperature_ramp.run(rs1)
    rs2 = mdsapi.insert_run_start(
        time=ttime.time(), beamline_id='replay testing', scan_id=2,
        beamline_config=mdsapi.insert_beamline_config({}, ttime.time()))
    events2 = temperature_ramp.run(rs2)
    dbfile = os.path.join(tempfile.gettempdir(), str(uuid.uuid1()) + '.db')

    h = History(dbfile)
    # making sure that replay and the test suite are pointing at the same
    # sql db is good mmmmkay?
    replay.history = h

    # set up some state for the first run start
    scan_id1 = random.randint(50000, 10000000)
    hdr_update_rate1 = random.randint(50000, 10000000)
    num_to_retrieve1 = random.randint(10, 20)
    # store some state
    state1 = {'x': 'Tsam', 'y': ['Tsam', 'point_det'], 'x_is_index': True}
    h.put(six.text_type(rs1.id), state1)
    returned_state = h.get(six.text_type(rs1.id))
    h.put('WatchForHeadersModel', {'update_rate': hdr_update_rate1})
    h.put('ScanIDSearchModel', {'scan_id': scan_id1})
    h.put('GetLastModel', {'num_to_retrieve': num_to_retrieve1})
    # store some more state
    h.put(six.text_type(rs2.id), {'y': ['Tsam', 'point_det'],
                                  'x_is_index': True})

    # open up replay
    app = QtApplication()
    ui = replay.create(replay.define_live_params())
    ui.title = ('Testing replay by manually triggering various models. '
                'Sit back and enjoy the show!')
    ui.show()
    hdr1 = db.find_headers(_id=rs1.id)[0]
    ui.muxer_model.header = hdr1
    # start replay so that it will stop in 4 seconds
    app.timed_call(4000, app.stop)
    app.start()

    try:
        # check that the observer between the muxer model and the scalar collection
        # is working properly
        assert six.text_type(ui.scalar_collection.dataframe_id) == six.text_type(rs1.id)
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x == state1['x']
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x_is_index == state1['x_is_index']
        y_matches = True
        for y in ui.scalar_collection.y:
            if y not in state1['y']:
                y_matches = False
        # make sure that the datasets that should be plotted are plotting
        assert y_matches
        # make sure that no extra datasets are plotting
        assert len(ui.scalar_collection.y) == len(state1['y'])
        # check state in the "live" mode
        assert ui.watch_headers_model.update_rate == hdr_update_rate1
        # check state loading in the search by scan_id model
        assert ui.scan_id_model.scan_id == scan_id1
        # check state loading in the get_last_model
        assert ui.get_last_model.num_to_retrieve == num_to_retrieve1
    except AssertionError:
        # make sure the app gets destroyed, even if there is an AssertionError
        # as this will cause problems later
        ui.close()
        app.destroy()
        raise

    # store some state for the 2nd header that differs from the first
    state2 = {'x': 'point_det', 'y': ['Tsam'], 'x_is_index': False}
    h.put(six.text_type(rs2.id), state2)
    hdr2 = db.find_headers(_id=rs2.id)[0]
    ui.muxer_model.header = hdr2


    # make sure that the things that are display are the things that we think
    # should be displayed. This requires starting/stopping replay
    app.timed_call(4000, app.stop)
    app.start()
    try:
        # check that the observer between the muxer model and the scalar collection
        # is working properly
        assert six.text_type(ui.scalar_collection.dataframe_id) == six.text_type(rs2.id)
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x == state2['x']
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x_is_index == state2['x_is_index']
        # check that the scalar collection is correctly loading plotting state
        for y in ui.scalar_collection.y:
            if y not in state2['y']:
                y_matches = False
        # make sure that the datasets that should be plotted are plotting
        assert y_matches
        # make sure that no extra datasets are plotting
        assert len(ui.scalar_collection.y) == len(state2['y'])
    except AssertionError:
        # make sure the app gets destroyed, even if there is an AssertionError
        # as this will cause problems later
        ui.close()
        app.destroy()
        raise

    def use_ram_state():
        # now set the plot state to be 'ram'
        ui.scalar_collection.use_ram_state = True
    app.timed_call(500, use_ram_state)
    app.timed_call(1000, app.stop)
    app.start()
    # make sure that it updated the disk state correctly and that no unexpected
    # updates happened
    assert ui.scalar_collection.use_disk_state == False
    assert ui.scalar_collection.use_ram_state == True

    # change the header back to the first header
    ui.muxer_model.header = hdr1
    # start/stop replay again
    app.timed_call(1000, app.stop)
    app.start()

    # make sure that the updates triggered correctly. It should now match the
    # state from the second run header
    try:
        # check that the observer between the muxer model and the scalar collection
        # is working properly
        assert six.text_type(ui.scalar_collection.dataframe_id) == six.text_type(rs1.id)
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x == state2['x']
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x_is_index == state2['x_is_index']
        # check that the scalar collection is correctly loading plotting state
        for y in ui.scalar_collection.y:
            if y not in state2['y']:
                y_matches = False
        # make sure that the datasets that should be plotted are plotting
        assert y_matches
        # make sure that no extra datasets are plotting
        assert len(ui.scalar_collection.y) == len(state2['y'])
    except AssertionError:
        # make sure the app gets destroyed, even if there is an AssertionError
        # as this will cause problems later
        ui.close()
        app.destroy()
        raise

    def use_disk_state():
        # now set the plot state to be 'ram'
        ui.scalar_collection.use_disk_state = True
    ui.scalar_collection.use_disk_state = True
    # make sure that the switch took place
    app.timed_call(500, use_disk_state)
    app.timed_call(1000, app.stop)
    app.start()
    # make sure that it updated the disk state correctly and that no unexpected
    # updates happened
    assert ui.scalar_collection.use_disk_state == True
    assert ui.scalar_collection.use_ram_state == False

    # the original state for run header 2 is now the state for run header 2.
    # Let's change the state for run header 2 on disk and switch back.
    h.put(six.text_type(rs2.id), state1)
    ui.muxer_model.header = hdr2
    app.timed_call(1000, app.stop)
    app.start()    # make sure that the things that are display are the things that we think
    # should be displayed. This requires starting/stopping replay
    try:
        # check that the observer between the muxer model and the scalar collection
        # is working properly
        assert six.text_type(ui.scalar_collection.dataframe_id) == six.text_type(rs2.id)
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x == state1['x']
        # check that the scalar collection is correctly loading plotting state
        assert ui.scalar_collection.x_is_index == state1['x_is_index']
        # check that the scalar collection is correctly loading plotting state
        for y in ui.scalar_collection.y:
            if y not in state1['y']:
                y_matches = False
        # make sure that the datasets that should be plotted are plotting
        assert y_matches
        # make sure that no extra datasets are plotting
        assert len(ui.scalar_collection.y) == len(state1['y'])
    except AssertionError:
        # make sure the app gets destroyed, even if there is an AssertionError
        # as this will cause problems later
        ui.close()
        app.destroy()
        raise

    ui.close()
    app.destroy()

