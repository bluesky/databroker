import pytest
from filestore.test.utils import fs_setup, fs_teardown
from filestore.fs import FileStoreMoving
import filestore.conf
from metadatastore.test.utils import mds_setup, mds_teardown
from databroker.core import register_builtin_handlers
from .utils import Command
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar, step_scan)


def setup_module(module):
    fs_setup()
    mds_setup()


def teardown_module(module):
    fs_teardown()
    mds_teardown()


def run_example_programmatically(example):
    events = example.run()
    assert isinstance(events, list)
    assert isinstance(events[0], dict)


def run_example_cmdline(example):
    command = Command('python {}'.format(example.__file__))
    command.run(timeout=1)


@pytest.mark.parametrize(
    'example',
    [temperature_ramp, multisource_event, image_and_scalar, step_scan]
)
def test_examples_programmatically(example):
    run_example_programmatically(example)
    run_example_cmdline(example)


def test_get_resource_uid():
    from databroker import DataBroker as db
    fs = FileStoreMoving(db.fs.config)
    old_fs = db.fs
    db.fs = fs
    register_builtin_handlers(fs)
    events = list(image_and_scalar.run())
    fs_res = set()
    run_start = events[0]['descriptor']['run_start']
    hdr = db[run_start['uid']]
    for ev in db.get_events(hdr, fill=False):
        for k in ['img', 'img_sum_x', 'img_sum_y']:
            dd = ev['data']
            if k in dd:
                fs_res.add(fs.resource_given_eid(dd[k])['uid'])

    assert fs_res == db.get_resource_uids(hdr)
    db.fs = old_fs
