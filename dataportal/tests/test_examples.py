import subprocess
from nose.tools import assert_true, assert_equal
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from metadatastore.api import Document
from filestore.utils.testing import fs_setup, fs_teardown
from metadatastore.utils.testing import mds_setup, mds_teardown
import uuid
db_name = str(uuid.uuid4())
conn = None
blc = None
examples = [temperature_ramp, multisource_event, image_and_scalar]


def setup():
    fs_setup()
    mds_setup()


def teardown():
    fs_teardown()
    mds_teardown()


def run_example_programmatically(example):
    events = example.run()
    assert_true(isinstance(events, list))
    assert_true(isinstance(events[0], Document))


def test_examples_programmatically():
    for example in examples:
        yield run_example_programmatically, example

