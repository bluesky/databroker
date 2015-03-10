import six
if six.PY2:
    # hide enaml imports from python 3
    import enaml
    from enaml.qt.qt_application import QtApplication
    from dataportal.replay import replay

from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
from dataportal.examples.sample_data import temperature_ramp, image_and_scalar
from dataportal.broker import DataBroker as db
import copy
from ..testing.decorators import skip_if

global hdr_temp_ramp, ev_temp_ramp
global hdr_img_scalar, ev_img_scalar
global app


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
    global app
    app = QtApplication()


@skip_if(not six.PY2)
def teardown():
    fs_teardown()
    mds_teardown()

@skip_if(not six.PY2)
def _replay_startup_tester(params=None, wait_time=1000):
    replay.create_and_show(params)
    app.timed_call(wait_time, app.stop)
    app.start()

@skip_if(not six.PY2)
def test_replay_startup():
    normal = replay.define_default_params()
    normal_small = copy.deepcopy(normal)
    normal_small['screen_size'] = 'small'
    live = replay.define_live_params()
    live_small = copy.deepcopy(live)
    live_small['screen_size'] = 'small'
    params = [(normal, 1000), (normal_small, 1000), (live, 4000),
              (live_small, 4000)]
    for p in params:
        yield _replay_startup_tester, p[0], p[1]
