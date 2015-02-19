from nose.tools import assert_true
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from metadatastore.api import Document

def run_example(example):
    events = example.run()
    assert_true(isinstance(events, list))
    assert_true(isinstance(events[0], Document))

def test_examples():
    for example in [temperature_ramp, multisource_event, image_and_scalar]:
        yield run_example, example
