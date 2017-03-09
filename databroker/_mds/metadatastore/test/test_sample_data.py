from ..examples.sample_data.common import apply_deadband, noisy
from ..examples.sample_data import (temperature_ramp,
                                    multisource_event)
import numpy as np
import itertools
import pytest


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
def test_sleepy_failures(mds_all, failing_time, mod):
    with pytest.raises(NotImplementedError):
        mod.run(mds_all, sleep=failing_time)


def test_multisource_event(mds_all):
    multisource_event.run(mds_all)


def test_temperature_ramp(mds_all):
    temperature_ramp.run(mds_all)
