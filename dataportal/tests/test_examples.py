import subprocess
from nose.tools import assert_true, assert_equal
from ..examples.sample_data import (temperature_ramp, multisource_event,
                                    image_and_scalar)
from metadatastore.api import Document


examples = [temperature_ramp, multisource_event, image_and_scalar]


def run_example_programmatically(example):
    events = example.run()
    assert_true(isinstance(events, list))
    assert_true(isinstance(events[0], Document))


def run_example_from_commandline(example):
    command = ['python', example.__file__]
    p = subprocess.Popen(command)
    return_code = p.wait()
    assert_equal(return_code, 0)  # successful execution


def test_examples_programmatically():
    for example in examples:
        yield run_example_programmatically, example


def test_examples_from_commandline():
    for example in examples:
        yield run_example_from_commandline, example
