from metadatastore.examples.sample_data.common import apply_deadband, noisy
from metadatastore.examples.sample_data import (temperature_ramp,
                                                multisource_event)
from metadatastore.utils.testing import mds_setup, mds_teardown
import numpy as np
import itertools
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
def _failing_helper(mod, delay_time):
    # any non-zero values should work
    mod.run(sleep=delay_time)

def test_sleepy_failures():
    failing_times = [-1, 1]
    modules = [temperature_ramp, multisource_event]
    for mod, failing_time in itertools.product(modules, failing_times):
        yield _failing_helper, mod, failing_time

def test_multisource_event():
    multisource_event.run()


def test_temperature_ramp():
    temperature_ramp.run()


if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
