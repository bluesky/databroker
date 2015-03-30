from nose.tools import assert_equal, assert_raises
from dataportal.examples.sample_data import step_scan, temperature_ramp
from metadatastore.utils.testing import mds_setup, mds_teardown
from dataportal import StepScan


def setup():
    mds_setup()
    step_scan.run()


def teardown():
    mds_teardown()


def test_step_scan():
    # use __getitem__
    df = StepScan[-1]
    assert_equal(list(df.columns), ['Tsam', 'time'])
    assert_equal(len(df), 3)

    # use find_headers
    df = StepScan.find_headers(data_key='Tsam')
    assert_equal(list(df.columns), ['Tsam', 'time'])
    assert_equal(len(df), 3)

def test_no_results():
    df = StepScan.find_headers(owner='no one')
    assert_equal(len(df), 0)

def test_raises_on_async_input():
    temperature_ramp.run()
    f = lambda: StepScan[-2:]
    assert_raises(ValueError, f)
