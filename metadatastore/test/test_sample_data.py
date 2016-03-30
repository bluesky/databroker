from metadatastore.examples.sample_data.common import apply_deadband, noisy
from metadatastore.examples.sample_data import (temperature_ramp,
                                                multisource_event)
from .utils import mds_setup, mds_teardown
import numpy as np
import itertools
import pytest


def setup_module(module):
    mds_setup()


def teardown_module(module):
    mds_teardown()


def test_bad_deadbands():
    # the -1 should throw a ValueError
    with pytest.raises(ValueError):
        apply_deadband(None, -1)


@pytest.mark.parametrize('v', [0, np.arange(0, 5)])
def test_noisy_for_smoke(v):
    noisy(v)


@pytest.mark.parametrize(
    'failing_time, mod',
    itertools.product([-1, 1], [temperature_ramp, multisource_event])
)
def test_sleepy_failures(failing_time, mod):
    with pytest.raises(NotImplementedError):
        mod.run(sleep=failing_time)


def test_multisource_event():
    multisource_event.run()


def test_temperature_ramp():
    temperature_ramp.run()
