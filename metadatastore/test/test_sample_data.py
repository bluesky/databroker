from metadatastore.examples.sample_data.common import apply_deadband, noisy
from metadatastore.examples.sample_data import (temperature_ramp,
                                                multisource_event)
from metadatastore.utils.testing import mds_setup, mds_teardown
import numpy as np
from nose.tools import raises

def setup():
    mds_setup()

def teardown():
    mds_teardown()

@raises(ValueError)
def test_bad_deadbands():
    # the -1 should throw a ValueError
    apply_deadband(None, -1)


def _noisy_helper(val):
    noisy(val)


def test_noisy_for_smoke():
    vals = [0, np.arange(0, 5)]
    for v in vals:
        yield _noisy_helper, v

@raises(NotImplementedError)
def _failing_sleepy_ramp_helper(delay_time):
    # any non-zero values should work
    temperature_ramp.run(sleep=delay_time)

def test_sleepy_ramp():
    failing_times = [-1, 1]
    for failing_time in failing_times:
        yield _failing_sleepy_ramp_helper, failing_time


def test_multisource_event():
    multisource_event.run()


def test_temperature_ramp():
    temperature_ramp.run()


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
