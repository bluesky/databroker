from nose.tools import assert_equal
from dataportal.examples.sample_data import step_scan
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
