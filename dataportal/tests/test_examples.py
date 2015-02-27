import subprocess
from nose.tools import assert_true, assert_equal
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from metadatastore.api import Document

from filestore.api import db_connect, db_disconnect
import uuid
db_name = str(uuid.uuid4())
conn = None
blc = None
examples = [temperature_ramp, multisource_event, image_and_scalar]


def setup():
    global conn
    db_disconnect()
    conn = db_connect(db_name, 'localhost', 27017)


def teardown():
    conn.drop_database(db_name)
    db_disconnect()


def run_example_programmatically(example):
    events = example.run()
    assert_true(isinstance(events, list))
    assert_true(isinstance(events[0], Document))


def test_examples_programmatically():
    for example in examples:
        yield run_example_programmatically, example

