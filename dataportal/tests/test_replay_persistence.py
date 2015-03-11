from nose.tools import assert_equal
from dataportal.replay.persist import History
import dataportal.replay.persist

OBJ_ID_LEN = 36
h = None


def setup():
    global h
    h = History(':memory:')


def test_history():
    run_id = ''.join(['a'] * OBJ_ID_LEN)
    # Simple round-trip: put and get
    config1 = {'plot_x': 'long', 'plot_y': 'island'}
    h.put(run_id, config1)
    result1 = h.get(run_id)
    assert_equal(result1, config1)

    # Put a second entry. Check that get returns most recent.
    config2 = {'plot_x': 'new', 'plot_y': 'york'}
    h.put(run_id, config2)
    result2 = h.get(run_id)
    assert_equal(result2, config2)
    # And get(..., 1) returns previous.
    result1 = h.get(run_id, 1)
    assert_equal(result1, config1)
