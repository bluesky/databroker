import pytest
from filestore.test.utils import fs_setup, fs_teardown
from metadatastore.test.utils import mds_setup, mds_teardown

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
